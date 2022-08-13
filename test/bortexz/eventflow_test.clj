(ns bortexz.eventflow-test
  (:require [clojure.test :as t]
            [clojure.core.async :as a]
            [bortexz.eventflow :as ef]))

(t/deftest incremental-pipeline
  (let [event-a {::ef/topic :topic-a}
        event-b {::ef/topic :topic-b}
        node-a {::ef/xform (map (fn [_] event-b))
                ::ef/topics {:topic-a #{:topic-b}}}
        node-b {::ef/xform (remove any?)
                ::ef/topics {:topic-b #{}}}]
    (t/testing "base-operations"
      (let [p (ef/incremental-pipeline ::ef/topic)]
        (ef/-add-node p :node-a node-a)
        (ef/-add-node p :node-b node-b)
        (t/is (empty? (ef/pending-events p)))
        (ef/publish p event-a)
        (t/is (= event-a (peek (ef/pending-events p))))
        (ef/flush p)
        (t/is (= event-b (peek (ef/pending-events p))))
        (ef/flush p)
        (t/is (empty? (ef/pending-events p)))
        (ef/remove-node p :node-a)
        (ef/publish p event-a)
        (ef/flush p)
        (t/is (empty? (ef/pending-events p)))))

    (t/testing "drain"
      (let [p (ef/incremental-pipeline ::ef/topic)]
        (ef/-add-node p :node-a node-a)
        (ef/-add-node p :node-b node-b)
        (ef/publish p event-a)
        (ef/drain p)
        (t/is (empty? (ef/pending-events p)))))

    (t/testing "parallel"
      (let [p (ef/incremental-pipeline ::ef/topic {::ef/parallel? true})]
        (ef/-add-node p :node-a node-a)
        (ef/-add-node p :node-b node-b)
        (ef/publish p event-a)
        (ef/flush p)
        (t/is (= event-b (peek (ef/pending-events p))))))

    (t/testing "if node throws, doesn't put new events"
      (let [p (ef/incremental-pipeline ::ef/topic)]
        (ef/-add-node p :node-a {::ef/xform (map (fn [_] (throw (ex-info "Error" {}))))
                                 ::ef/topics {:topic-a #{:topic-b}}})
        (ef/publish p event-a)
        (t/is (thrown? clojure.lang.ExceptionInfo (ef/flush p)))
        (t/is (empty? (ef/pending-events p)))))))

(def ^:private wait 10)

(t/deftest async-pipeline
  (let [event-a {::ef/topic :topic-a}
        event-b {::ef/topic :topic-b}]
    (t/testing "base operations"
      (let [event_ (atom nil)
            node-a {::ef/xform (map (fn [_] event-b))
                    ::ef/topics {:topic-a #{:topic-b}}}
            node-b {::ef/xform (comp (map (fn [ev] (reset! event_ ev)))
                                     (remove any?))
                    ::ef/topics {:topic-b #{}}}
            p (ef/async-pipeline ::ef/topic)]
        (-> p
            (ef/add-node :node-a node-a)
            (ef/add-node :node-b node-b)
            (ef/publish event-a))
        (a/<!! (a/timeout wait))
        (t/is (= event-b @event_))
        (ef/remove-node p :node-a)
        (reset! event_ nil)
        (ef/publish p event-a)
        (a/<!! (a/timeout wait))
        (t/is (nil? @event_))))

    (t/testing "ex-handler"
      (let [exception_ (atom nil)
            event_ (atom nil)
            node-a {::ef/xform (map (fn [_] (throw (ex-info "error" {}))))
                    ::ef/topics {:topic-a #{:topic-b}}}
            node-b {::ef/xform (comp (map (fn [ev] (reset! event_ ev)))
                                     (remove any?))
                    ::ef/topics {:topic-b #{}}}
            p (ef/async-pipeline ::ef/topic {::ef/ex-handler (fn [ex]
                                                               (reset! exception_ ex))})]
        (-> p
            (ef/add-node :node-a node-a)
            (ef/add-node :node-b node-b)
            (ef/publish event-a))
        (a/<!! (a/timeout wait))
        (t/is (nil? @event_))
        (t/is (= :node-a (:node-id (ex-data @exception_))))
        (t/is (= event-a (:event (ex-data @exception_))))))
    
    (t/testing "parallelism"
      (let [exception_ (atom nil)
            events_ (atom #{})
            node-a {::ef/xform (map (fn [event] 
                                      (swap! events_ conj (:data event))
                                      (Thread/sleep (* wait 2)) ; keep thread busy
                                      ))
                    ::ef/topics {:topic-a #{}}
                    ::ef/async {::ef/parallelism 2
                                ::ef/thread? true}}
            p (ef/async-pipeline ::ef/topic {::ef/ex-handler (fn [ex]
                                                               (reset! exception_ ex))})]
        (-> p
            (ef/add-node :node-a node-a)
            (ef/publish {::ef/topic :topic-a :data 1})
            (ef/publish {::ef/topic :topic-a :data 2})
            (ef/publish {::ef/topic :topic-a :data 3}))
        (a/<!! (a/timeout wait))
        (t/is (= #{1 2} @events_))
        (a/<!! (a/timeout (* 3 wait)))
        (t/is (= #{1 2 3} @events_))))))
