(ns simple-batch-processor.core
  (require [com.climate.claypoole :as cp]))


(defn process-queue [threadpool queue handler options]
  (when (pos? (count @queue))
    (let [batch (take (:batch-size options) @queue)]
      (swap! queue (partial drop (count batch)))
      (cp/future threadpool (handler batch)))))


(defn create-timeout-handler [threadpool queue handler options]
  (future
    (Thread/sleep (:timeout options))
    (locking queue
      (process-queue threadpool queue handler options))))


(defn stream->batch [threadpool queue timeout-handler handler options message]
  (locking queue
    (swap! queue conj message)
    (when (future-done? @timeout-handler)
      (reset! timeout-handler
              (create-timeout-handler threadpool queue handler options)))
    (when (= (:batch-size options) (count @queue))
      (future-cancel @timeout-handler)
      (process-queue threadpool queue handler options))))


(defn shutdown
  "Permanently shuts down threadpool associated with given processor fn."
  [processor-fn]
  (cp/shutdown (:threadpool (meta processor-fn))))


(defn stream->batch-processor-fn
  "Wraps handler with stream->batch processor and attaches threadpool
   as metadata."
  [handler options]
  (let [threadpool (cp/threadpool (:threads options))
        queue (atom (list))
        timeout-handler (atom (future))
        processor-fn (partial stream->batch
                              threadpool
                              queue
                              timeout-handler
                              handler
                              options)]
    (with-meta processor-fn {:threadpool threadpool})))


(defmacro with-stream->batch
  "Create temporary binding wrapping the given handler function.
   Requires a symbol for the handler function, so it must already be bound
   by other means.

   (with-stream->batch [tmp-proc {:batch-size 5 :threads 2 :timeout 1000}]
     (doseq [x (range 17)]
       (tmp-proc x))"
  [[handler options] & body]
  `(let [~handler (stream->batch-processor-fn ~handler ~options)]
     ~@body
     (cp/shutdown (:threadpool (meta ~handler)))))


(defmacro defstream->batch
  "Define a new stream->batch processor with batch-size and timeout.

   options
      :batch-size     <int> max batch count
      :threads      <int> max handler threads
      :timeout      <int> timeout in ms

   (defstream->batch message-processor
      my-handler-fn
      {:batch-size 100 :threads 2 :timeout 1000})

   (doseq [x (range 250)]
     (message-processor x))"
  [id handler options]
  `(let [processor-fn# (stream->batch-processor-fn ~handler ~options)]
     (def ~id processor-fn#)))
