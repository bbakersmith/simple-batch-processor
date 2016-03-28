(ns simple-batch-processor.core
  (:require [com.climate.claypoole :as cp]))


(defn ^:private process-queue [threadpool queue handler options]
  (when (pos? (count @queue))
    (let [batch (take (:batch-size options) @queue)]
      (swap! queue (partial drop (count batch)))
      (cp/future threadpool (handler batch)))))


(defn ^:private create-timeout-handler [threadpool queue handler options]
  (future
    (Thread/sleep (:timeout options))
    (locking queue
      (process-queue threadpool queue handler options))))


(defn ^:private stream->batch-processor-fn
  [threadpool queue timeout-handler handler options message]
  (locking queue
    (swap! queue conj message)
    (when (future-done? @timeout-handler)
      (reset! timeout-handler
              (create-timeout-handler threadpool queue handler options)))
    (when (= (:batch-size options) (count @queue))
      (future-cancel @timeout-handler)
      (process-queue threadpool queue handler options))))


(defn queue-contents
  "Returns the current contents of the queue."
  [processor-fn]
  (deref (:queue (meta processor-fn))))


(defn queue-size
  "Returns the current queue size"
  [processor-fn]
  (count (queue-contents processor-fn)))


(defn purge-queue
  "Remove all items from the queue without processing."
  [processor-fn]
  (let [queue (:queue (meta processor-fn))]
    (locking queue
      (reset! queue (list)))))


(defn shutdown
  "Permanently shuts down threadpool associated with given processor fn."
  [processor-fn]
  (cp/shutdown (:threadpool (meta processor-fn))))


(defn stream->batch
  "Returns a batch processing function.

   options
      :batch-size   <int> max batch count
      :threads      <int> max handler threads
      :timeout      <int> timeout in ms"
  [handler options]
  (let [threadpool (cp/threadpool (:threads options))
        queue (atom (list))
        timeout-handler (atom (future))
        processor-fn (partial stream->batch-processor-fn
                              threadpool
                              queue
                              timeout-handler
                              handler
                              options)]
    (with-meta processor-fn {:threadpool threadpool
                             :queue queue})))


(defmacro with-stream->batch
  "Temporary scoped binding for batch processing function,
   with automatic threadpool shutdown.

   options
      :batch-size   <int> max batch count
      :threads      <int> max handler threads
      :timeout      <int> timeout in ms

   (with-stream->batch [tmp-proc (fn [batch] (do-something batch))
                        {:batch-size 5 :threads 2 :timeout 1000}]
     (doseq [x (range 17)]
       (tmp-proc x))"
  [[id handler options] & body]
  `(let [~id (stream->batch ~handler ~options)]
     ~@body
     (cp/shutdown (:threadpool (meta ~id)))))
