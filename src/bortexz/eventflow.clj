(ns bortexz.eventflow
  (:refer-clojure :exclude [flush])
  (:require [clojure.spec.alpha :as s]
            [medley.core :as medley]
            [clojure.core.async :as a]
            [bortexz.utils.core :as uc]
            [bortexz.utils.async :as ua]))

;;
;; protocols
;;

(defprotocol -Pipeline
  (-add-node [this node-id node-config])
  (-remove-node [this node-id]))

(defprotocol -Publish
  (-publish [this event]))

(defprotocol -AsyncPipeline
  (-publish-ch [this ch]))

(defprotocol -IncrementalPipeline
  (-flush [this])
  (-pending-events [this]))

;;
;; specs
;;

(s/def ::topic keyword?)
(s/def ::event (s/keys :req [::topic]))

(s/def ::emits (s/coll-of ::topic))
(s/def ::xform fn?)
(s/def ::topics (s/map-of ::topic ::emits))
(s/def ::parallelism pos-int?)
(s/def ::thread? boolean?)
(s/def ::topic-buf-fn
  (s/fspec :args (s/cat :topic ::topic)))
(s/def ::combine-chs-fn
  (s/fspec :args (s/cat :topic-chs (s/map-of ::topic ua/chan?))))

(s/def ::async (s/keys :opt [::parallelism ::thread? ::topic-buf-fn ::combine-chs-fn]))
(s/def ::node-config (s/keys :req [::xform ::topics]
                             :opt [::async]))

(s/def ::parallel? boolean?)
(s/def ::ex-handler
  (s/fspec :args (s/cat :exception any?)))

;;
;; api
;;

(defn incremental-pipeline
  "Creates an incremental pipeline. When being published to, events are stored inside the pipeline until [[flush]] is 
   called. Flush will propagate the first event of the queue through the pipeline, executing all nodes subscribed to the 
   event's `::topic`. This step might generate more events from the processing nodes that are added into the pipeline.
   Use [[drain]] to flush the pipeline in loop until no more events are emited.
   
   Accepts the following options (namespaced)
   - `::parallel?` if true, nodes are executed in parallel during [[flush]].
   
   Not thread safe currently, this pipeline is intended to be run from a single thread, behaviour in multithreading is 
   undefined."
  ([] (incremental-pipeline {}))
  ([{::keys [parallel?] :or {parallel? false}}]
   (let [state_ (atom {:nodes {} ; {<node-id> <xform f>}
                       :topics {} ; {<topic> #{<node-id>}}
                       })

         events_ (atom (medley/queue))

         execute-node (fn [node-id f event]
                        (try (f [] event)
                             (catch Throwable t
                               (throw
                                (ex-info "exception executing node xform"
                                         {:node-id node-id
                                          :event event
                                          :exception t})))))]
     (reify
       -Pipeline
       (-add-node [this node-id {node-topics ::topics node-xform ::xform}]
         (let [old
               (medley/deref-swap!
                state_
                (fn [{:keys [nodes] :as state}]
                  (if (get nodes node-id)
                    state
                    (-> state
                        (update :nodes assoc node-id (node-xform conj))
                        (update :topics (fn [t->nodes]
                                          (reduce (fn [m t]
                                                    (update m t (fnil conj #{}) node-id))
                                                  t->nodes
                                                  (keys node-topics))))))))]
           (when (get (:nodes old) node-id)
             (throw (ex-info "node-id already exists" {:node-id node-id})))
           this))

       (-remove-node [this node-id]
         (let [old (medley/deref-swap!
                    state_
                    (fn [{:keys [nodes] :as state}]
                      (if-not (get nodes node-id)
                        state
                        (-> state
                            (update :nodes dissoc node-id)
                            (update :topics (fn [ts]
                                              (->> ts
                                                   (medley/map-vals (fn [node-set] (disj node-set node-id)))
                                                   (medley/remove-vals empty?))))))))]
           (if-let [node-xf (get (:nodes old) node-id)]
             (let [finished-events (node-xf [])]
               (swap! events_ into finished-events))
             (throw (ex-info "node-id does not exist" {:node-id node-id})))
           this))

       -Publish
       (-publish [_ event]
         (swap! events_ conj event))

       -IncrementalPipeline
       (-flush [this]
         (let [event (peek (medley/deref-swap! events_ pop))
               t (::topic event)
               state @state_
               nodes (select-keys (:nodes state) (get (:topics state) t)) 
               next-evs (if parallel?
                          (->> nodes
                               (mapv (fn [[node-id f]] (future (execute-node node-id f event))))
                               (mapcat deref))
                          (mapcat (fn [[node-id f]] (execute-node node-id f event)) nodes))]
           (swap! events_ into next-evs)
           this))

       (-pending-events [_] @events_)))))

(def ^:private default-async
  {::parallelism 1
   ::thread? false
   ::topic-buf-fn (constantly nil)
   ::combine-chs-fn (fn [chm] (a/merge (vals chm)))})

(defn async-pipeline
  "Creates an async pipeline"
  ([] (async-pipeline {}))
  ([{::keys [topic-buf-fn ex-handler] :or {topic-buf-fn (constantly nil)
                                           ex-handler uc/uncaught-exception}}]
   (let [topics_ (atom {}) ; topic->mult
         topics-fx!__ (atom (delay {})) ; (delay {<topic> <node-ids set>}) coordinate add/remove topic mults on topics_
         nodes_ (atom {}) ; {<node> (delay <side-effects add node> (delay <side effects removes node>))})}

         execute-node (fn [node-id f event]
                        (try (if event (f [] event) (f []))
                             (catch Throwable t
                               (ex-handler
                                (ex-info "Exception thrown while processing node"
                                         {:node-id node-id
                                          :event event
                                          :exception t}))
                               nil)))]

     (reify
       -Pipeline
       (-add-node [this node-id {::keys [topics xform async]}]
         (let [{::keys [parallelism thread? combine-chs-fn] node-topic-buf-fn ::topic-buf-fn}
               (merge default-async async)

               [o n]
               (swap-vals!
                nodes_
                (fn [m]
                  (if (get m node-id)
                    m
                    ; add self node
                    (assoc
                     m
                     node-id
                     (delay
                      ; Make sure mults will exist, and add topic connections
                      (uc/chain-fx!
                       topics-fx!__
                       (fn [topic->node-ids]
                         (reduce (fn [t->ns t]
                                   (when-not (get @topics_ t)
                                     (let [ch (a/chan (topic-buf-fn t))
                                           m (ua/mult ch)]
                                       (swap! topics_ assoc t m)))
                                   (update t->ns t (fnil conj #{}) node-id))
                                 topic->node-ids
                                 (keys topics))))

                      ; Create and connect node
                      (let [topic-mults (select-keys @topics_ (keys topics))
                            topic-chs (medley/map-kv-vals
                                       (fn [t _] (a/chan (node-topic-buf-fn t)))
                                       topics)
                            input-ch (combine-chs-fn topic-chs)
                            workers (mapv
                                     (fn [_]
                                       (let [f (xform conj)]
                                         (if thread?
                                           (a/thread
                                             (loop []
                                               (let [v (a/<!! input-ch)
                                                     os (execute-node node-id f v)]
                                                 (run! (fn [o]
                                                         (when-let [t (get @topics_ (::topic o))]
                                                           (a/>!! (a/muxch* t) o)))
                                                       os)
                                                 (when (some? v) (recur)))))
                                           (a/go-loop []
                                             (let [v (a/<! input-ch)
                                                   os (execute-node node-id f v)]
                                               (doseq [o os]
                                                 (when-let [t (get @topics_ (::topic o))]
                                                   (a/>! (a/muxch* t) o)))
                                               (when (some? v) (recur)))))))
                                     (range parallelism))]
                        (run! (fn [[t ch]] (a/tap (get topic-mults t) ch false)) topic-chs)
                        (delay
                         ; untap/close chs, signals removal
                         (run! (fn [[t ch]]
                                 (a/untap (get @topics_ t) ch)
                                 (a/close! ch))
                               topic-chs)
                         ; Wait for workers
                         (run! (fn [wch] (a/<!! wch)) workers)
                         ; Remove from topic connections, and cleanup empty topics
                         (uc/chain-fx!
                          topics-fx!__
                          (fn [topic->node-ids]
                            (reduce (fn [t->ns t]
                                      (let [t->ns (update t->ns t disj node-id)]
                                        (when (empty? (get t->ns t))
                                          (a/close! (a/muxch* (get @topics_ t)))
                                          (swap! topics_ dissoc t))
                                        t->ns))
                                    topic->node-ids
                                    (keys topics))))
                         ; Remove self from nodes
                         (swap! nodes_ dissoc node-id))))))))]
           (if (get o node-id)
             (throw (ex-info "node-id already exists" {:node-id node-id}))
             (force (get n node-id))))
         this)

       (-remove-node [this node-id]
         (if-let [node (get @nodes_ node-id)]
           (force (force node))
           (throw (ex-info "node-id does not exist" {:node-id node-id})))
         this)

       -Publish
       (-publish [this event]
         (when-let [mult (get @topics_ (::topic event))]
           (a/>!! (a/muxch* mult) event))
         this)

       -AsyncPipeline
       (-publish-ch [_ events-ch]
         (a/go-loop []
           (when-let [event (a/<! events-ch)]
             (when-let [mult (get @topics_ (::topic event))]
               (a/>! (a/muxch* mult) event))
             (recur))))))))

(defn add-node 
  [pipeline node-id node-config]
  (-add-node pipeline node-id node-config))

(defn remove-node
  [pipeline node-id]
  (-remove-node pipeline node-id))

(defn publish 
  [pipeline event]
  (-publish pipeline event))

(defn publish-ch
  [async-pipeline events-ch]
  (-publish-ch async-pipeline events-ch))

(defn flush
  [inc-pipeline]
  (-flush inc-pipeline))

(defn pending-events
  [inc-pipeline]
  (-pending-events inc-pipeline))

(defn drain
  [inc-pipeline]
  (while (seq (pending-events inc-pipeline))
    (flush inc-pipeline)))
