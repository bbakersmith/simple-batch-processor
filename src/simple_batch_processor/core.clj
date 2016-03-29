(ns simple-batch-processor.core
  (:require [com.climate.claypoole :as cp])
  (:import java.util.concurrent.LinkedBlockingQueue))


(defn ^:private process-queue [threadpool queue handler options]
  (let [batch (java.util.Vector.)
        _ (.drainTo queue batch (:batch-size options))
        batch (into [] batch)]
    (when (seq batch)
      (cp/future threadpool (handler batch)))))


(defn ^:private create-timeout-handler
  [threadpool queue timeout-handler handler options]
  (swap!
   timeout-handler
   #(do
      (future-cancel %)
      (future
       (Thread/sleep (:timeout options))
       (locking queue
         (process-queue threadpool queue handler options))))))


(defn ^:private stream->batch-processor-fn
  [threadpool queue timeout-handler handler options message]
  (.put queue message)
  (locking queue
    (when (>= (.size queue) (:batch-size options))
      (future-cancel @timeout-handler)
      (process-queue threadpool queue handler options)))
  (when (future-done? @timeout-handler)
    (create-timeout-handler
     threadpool queue timeout-handler handler options)))


(defn queue-contents
  "Returns the current contents of the queue."
  [processor-fn]
  (into [] (.toArray (:queue (meta processor-fn)))))


(defn queue-size
  "Returns the current queue size"
  [processor-fn]
  (.size (:queue (meta processor-fn))))


(defn purge-queue
  "Remove all items from the queue without processing."
  [processor-fn]
  (.clear (:queue (meta processor-fn))))


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
        queue (LinkedBlockingQueue.)
        timeout-handler (atom (future))
        processor-fn (partial stream->batch-processor-fn
                              threadpool
                              queue
                              timeout-handler
                              handler
                              options)]
    (with-meta processor-fn {:threadpool threadpool
                             :timeout-handler timeout-handler
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
