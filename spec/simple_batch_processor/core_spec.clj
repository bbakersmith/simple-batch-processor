(ns simple-batch-processor.core-spec
  (:require [speclj.core :refer :all]
            [simple-batch-processor.core :refer :all]))


(describe "stream->batch"
  (with-stubs)

  (before
    (def handler-calls (atom []))
    (defstream->batch message-processor
      (fn [batch] (swap! handler-calls conj batch))
      {:batch-size 5 :timeout 100 :threads 4})

    (def alternate-handler-calls (atom []))
    (defstream->batch alternate-processor
      (fn [batch] (swap! alternate-handler-calls conj batch))
      {:batch-size 3 :timeout 100 :threads 4})

    (def slow-handler-calls (atom []))
    (defstream->batch slow-processor
      (fn [batch] (Thread/sleep 100) (swap! slow-handler-calls conj batch))
      {:batch-size 5 :timeout 200 :threads 2}))

  (it "should execute a batch when queue reaches batch-size"
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

  (it "should start timeout at time of last batch-size trigger"
    (doseq [x (range 9)]
      (message-processor x))
    (Thread/sleep 75)
    ;; triggering another batch-size should reset timeout
    ;; to fire at 175 instead of 100
    (doseq [x (range 5)]
      (message-processor x))
    (Thread/sleep 75)
    ;; at 150 timeout should still not have fired
    (should= 2 (count @handler-calls))
    (Thread/sleep 50)
    ;; by 200 it should have fired
    (should= 3 (count @handler-calls)))

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
             (sort (flatten @alternate-handler-calls))))

  (it "should limit the number of threads"
    (doseq [x (range 22)]
      (slow-processor x))
    ;; with handler threads limited to 2 and blocking for 100ms,
    ;; after 150ms only 2 of 5 should have finished
    (Thread/sleep 150)
    (should= 2 (count @slow-handler-calls))
    ;; after 250ms another 2 should have finished
    (Thread/sleep 100)
    (should= 4 (count @slow-handler-calls))
    ;; after 450ms the 200ms timeout should have completed the remainder
    (Thread/sleep 200)
    (should= 5 (count @slow-handler-calls)))

  (it "should allow threadpools to be shut down"
    (let [tp (:threadpool (meta message-processor))]
      (should= false (.isShutdown tp))
      (shutdown message-processor)
      (should= true (.isShutdown tp))))

  (it "should allow temporary processors"
    (let [tmp-handler-calls (atom [])
          tmp-proc (fn [batch] (swap! tmp-handler-calls conj batch))]
      (with-stream->batch [tmp-proc {:batch-size 5 :threads 2 :timeout 1000}]
        (doseq [x (range 15)]
          (tmp-proc x)))
      (Thread/sleep 200)
      (should= 3 (count @tmp-handler-calls)))))
