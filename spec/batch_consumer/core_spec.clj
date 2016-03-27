(ns batch-consumer.core-spec
  (:require [speclj.core :refer :all]
            [batch-consumer.core :refer :all]))


(describe "stream->batch"
  (with-stubs)

  (before (dosync (ref-set queue (list))))

  (around [it]
    (with-redefs [config (assoc config :timeout 100)]
      (it)))

  (with handler-calls (atom []))
  (with handler-stub (fn [batch] (swap! @handler-calls conj batch)))

  (it "should execute a batch when queue reaches max-size"
    (doseq [x (range 20)]
      (stream->batch @handler-stub x))
    (Thread/sleep 200)
    (should= [[4 3 2 1 0]
              [9 8 7 6 5]
              [14 13 12 11 10]
              [19 18 17 16 15]]
             @@handler-calls))
  
  (it "should execute a batch on timeout"
    (stream->batch @handler-stub 666)
    (Thread/sleep 200)
    (stream->batch @handler-stub 777)
    (Thread/sleep 200)
    (should= [[666] [777]]
             @@handler-calls)))
