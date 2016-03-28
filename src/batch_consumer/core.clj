(ns batch-consumer.core
  (require [com.climate.claypoole :as cp]))


(def threadpools (atom {}))


(defn process-queue [threadpool queue handler options]
  (when (pos? (count @queue))
    (let [batch (take (:max-size options) @queue)]
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
    (when (= (:max-size options) (count @queue))
      (future-cancel @timeout-handler)
      (process-queue threadpool queue handler options))))


(defn shutdown [id]
  (cp/shutdown (get @threadpools id)))


(defmacro defstream->batch
  "Define a new stream->batch processor with max-size and timeout.

   options
      :max-size     <int> max batch count
      :threads      <int> max handler threads
      :timeout      <int> timeout in ms

   (defstream->batch message-processor
      my-handler-fn
      {:max-size 100 :threads 2 :timeout 1000})

   (doseq [x (range 250)]
     (message-processor x))"
  [id handler options]
  `(let [threadpool# (cp/threadpool (:threads ~options))
         queue# (atom (list))
         timeout-handler# (atom (future))
         ns-qualified-id# (symbol (name (ns-name *ns*)) (name '~id))]
     (swap! threadpools assoc ns-qualified-id# threadpool#)
     (def ~id
       (partial stream->batch
                threadpool#
                queue#
                timeout-handler#
                ~handler
                ~options))))


(defmacro with-stream->batch [[handler options] & body]
  `(let [threadpool# (cp/threadpool (:threads ~options))
         queue# (atom (list))
         timeout-handler# (atom (future))
         ~handler (partial stream->batch
                           threadpool#
                           queue#
                           timeout-handler#
                           ~handler
                           ~options)]
     ~@body
     (cp/shutdown threadpool#)))
