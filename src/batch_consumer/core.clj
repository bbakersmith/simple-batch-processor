(ns batch-consumer.core)


(defn process-queue [queue handler config]
  (when (pos? (count @queue))
    (let [batch (take (:max-size config) @queue)]
      (swap! queue (partial drop (count batch)))
      (future (handler batch)))))


(defn create-timeout-handler [queue handler config]
  (future (Thread/sleep (:timeout config))
          (locking queue
            (process-queue queue handler config))))


(defn stream->batch [queue timeout-handler handler config message]
  (locking queue
    (swap! queue conj message)
    (when (future-done? @timeout-handler)
      (reset! timeout-handler (create-timeout-handler queue handler config)))
    (when (= (:max-size config) (count @queue))
      (process-queue queue handler config))))


(defmacro defstream->batch
  "Define a new stream->batch processor with max-size and timeout.

   (defstream->batch message-processor
      my-handler-fn
      {:max-size 100 :timeout 1000})

   (doseq [x (range 200)]
     (message-processor x))"
  [id handler config]
  `(do
     (def queue# (atom (list)))
     (def timeout-handler# (atom (future)))
     (def ~id
       (partial stream->batch queue# timeout-handler# ~handler ~config))))
