# eventflow

Event processing pipelines for Clojure.

## Features:

- Event processors as transducers that take events and (optionally) produce events.

- **Incremental pipeline**, a pipeline implementation for controlled execution of events. Call `flush` to propagate the next event through it's processors (sequentially or in parallel), use `drain` to flush a pipeline until it has no more events left. Useful to run simulations or test your pipeline with a predefined set of events.
    
- **Async pipeline**, assembles a pipeline using core.async chans that asynchronously processes events. A chan can be published to the pipeline using `publish-ch`. Processor nodes can be added or removed anytime from the pipeline. For when you're ready to go realtime.

## Creating nodes
Nodes are expressed as maps like:
```clojure
(require '[bortexz.eventflow :as ef])

{::ef/xform (map (fn [{::ef/keys [topic] :as event}]
                   (case topic
                     :input-1 {::ef/topic :output-1}
                     :input-2 {::ef/topic :output-2})))
 ::ef/topics {:input-1 #{:output-1}
              :input-2 #{:output-2}}
 ::ef/async {::ef/parallelism 8
             ::ef/thread? true
             ::ef/topic-buf-fn (fn [topic] (a/sliding-buffer 1))
             ::ef/combine-chs-fn (fn [{:keys [input-1 input-2]}]
                                   (a/merge [input-1 input-2]))}}
```

### Description
- `::ef/xform` transducer fn that accepts events and may produce events.
- `::ef/topics` map from subscribed topics to emitted topics. Currently emitted topics are not used/verified, and are merely used as metadata (e.g for printing a diagram of the pipeline)
- `::ef/async` async properties of the node when used in an async pipeline:
    - `::ef/parallelism` number of workers to consume events that this node is subscribed to.
    - `::ef/thread?` when true, workers will use a real thread to process events. Otherwise, go-loops are used.
    - `::ef/topic-buf-fn` 1-arity fn that returns a core.async buffer/int/nil added to chan of given topic.
    - `::ef/combine-chs-fn` 1-arity fn that returns a core.async chan that combines each topic's ch to produce the events feeded into xform. Accepts a map `{<topic> <ch>}`, and defaults to `(core.async/merge (vals chs))`.

## Building pipelines
```clojure

; This fn will work with both types of pipelines, so pipeline building can be reused across incremental/async pipelines
(defn build-pipeline [p]
  (-> p 
      (add-node :node-1 {::ef/xform ...})
      (add-node :node-2 {...})
      (add-node :node-3 {...})))

(def inc-p (ef/incremental-pipeline {::ef/topic-fn :topic
                                     ::ef/parallel? true
                                    }))

(def async-p (ef/async-pipeline {::ef/topic-fn :topic
                                 ::ef/topic-buf-fn (fn [topic] (a/sliding-buffer 1))
                                 ::ef/ex-handler (fn [ex] ...)}))

(build-pipeline inc-p)
(build-pipeline async-p)

```

Options for each pipeline:
- Incremental
    - `::ef/topic-fn` fn executed with an event that returns a topic
    - `::ef/parallel?` when true, processing nodes for next event will happen in parallel
- Async
    - `::ef/topic-fn` fn executed with an event that returns a topic
    - `::ef/topic-buf-fn` returns async buffer/int/nil, to be used as buffer of each topics internal ch.
    - `::ef/ex-handler` exception handler for exceptions happening inside node transducers. Defaults to use the uncaught exception handler of thread.

## Executing pipelines

### Incremental pipelines 
```clojure
(defn inc-p (ef/incremental-pipeline {::ef/topic-fn :topic}))

;; Publish events to the pipeline. It doesn't execute nodes, only stores the events in an internal queue.
(ef/publish inc-p {:topic :topic-a})

;; For processing the next event of the queue:
(ef/flush inc-p) ; blocking until the event is processed by all nodes.

;; You can get the current internal queue of events
(ef/pending-events inc-p)

;; Draining the pipeline will flush in loop until events queue is empty. Careful with cycles!
(ef/drain inc-p) ; blocking until all events are processed.
```

### Async pipelines
```clojure 
(defn inc-p (ef/incremental-pipeline {::ef/topic-fn :topic}))

;; Publish an event into the pipeline. If topic exists (i.e has any subscriber), then uses blocking >!! 
;; to publish the event for asynchronous processing. 
(ef/publish inc-p {:topic :topic-a})

;; You can publish into an async pipeline from an async chan directly:
(ef/publish-ch inc-p _chan-producing-events)

```

## License

Copyright © 2022 Alberto Fernandez

Distributed under the Eclipse Public License version 1.0.
