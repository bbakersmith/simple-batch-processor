(ns batch-consumer.core
  (require [clj-time.core :as t]))


;; stream->batch handler with timeout and max-size
;; single-threaded if possible


;; when timer-start is nil
;;  log current time

;; put item on queue

;; for each message
;;  if time > timer-start + timeout || queue size >= max-size
;;   process everything on the queue
;;   set time-start to nil
;;  else
;;   put item on queue


(def config
  {:max-size 5
   :timeout 1000})


(def timeout-handler (atom (future)))
(def queue (ref (list)))


(defn process-queue [handler]
  (dosync
   (when (pos? (count @queue))
     ;; could restrict to max-size, (take (:max-size config) @queue)
     ;; but processing the queue completely protects against unforseen race
     ;; conditions which should never happen
     (let [batch @queue]
       (alter queue (partial drop (:max-size config)))
       (future (handler batch))))))


(defn create-timeout-handler [handler]
  (reset! timeout-handler
          (future (Thread/sleep (:timeout config))
                  (process-queue handler))))


(defn stream->batch [handler message]
  (dosync (alter queue conj message))
  (when (future-done? @timeout-handler)
    (create-timeout-handler handler))
  (when (= (:max-size config) (count @queue))
    (process-queue handler)))


;; if no timeout future, create one
;; if max-size reached, cancel timeout future

;; (def a (future (Thread/sleep 10000)))
;; (future-cancel a)
;; (future-done? a)
;; (future-cancelled? a)

;; (def a (ref (list)))
;; (dosync (alter a conj 2))
;; (dosync (alter a (partial drop 1)))
