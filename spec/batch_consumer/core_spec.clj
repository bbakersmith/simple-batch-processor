(ns batch-consumer.core-spec
  (:require [speclj.core :refer :all]
            [batch-consumer.core :refer :all]))


(describe "stream->batch"
  (with-stubs)

  (before
    (def handler-calls (atom []))
    (defstream->batch message-processor
      (fn [batch] (swap! handler-calls conj batch))
      {:max-size 5 :timeout 100})

    (def alternate-handler-calls (atom []))
    (defstream->batch alternate-processor
      (fn [batch] (swap! alternate-handler-calls conj batch))
      {:max-size 3 :timeout 100}))

  (it "should execute a batch when queue reaches max-size"
    (doseq [x (range 20)]
      (message-processor x))
    (Thread/sleep 200)
    (should= 4 (count @handler-calls))
    (should= #{[4 3 2 1 0]
               [9 8 7 6 5]
               [14 13 12 11 10]
               [19 18 17 16 15]}
             (into #{} @handler-calls)))

  (it "should execute a batch on timeout"
    (message-processor 666)
    (Thread/sleep 200)
    (message-processor 777)
    (Thread/sleep 200)
    (should= [[666] [777]]
             @handler-calls))

  (it "should be thread safe"
    (doseq [x (range 22)]
      (future (message-processor x)))
    (Thread/sleep 200)
    (should= 5 (count @handler-calls))
    (should= (into [] (range 22))
             (sort (flatten @handler-calls))))

  (it "should allow multiple stream->batch processors"
    (future (doseq [x (range 22)]
              (future (message-processor x))))
    (future (doseq [y (range 50 60)]
              (future (alternate-processor y))))

    (Thread/sleep 200)
    (should= 5 (count @handler-calls))
    (should= (into [] (range 22))
             (sort (flatten @handler-calls)))
    (should= 4 (count @alternate-handler-calls))
    (should= (into [] (range 50 60))
             (sort (flatten @alternate-handler-calls)))))
