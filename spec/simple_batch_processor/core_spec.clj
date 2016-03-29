(ns simple-batch-processor.core-spec
  (:require [speclj.core :refer :all]
            [simple-batch-processor.core :refer :all]))


(describe "stream->batch"
  (with-stubs)

  (before
    (def handler-calls (atom []))
    (def message-processor
      (stream->batch
       (fn [batch] (swap! handler-calls conj batch))
       {:batch-size 5 :timeout 100 :threads 4}))

    (def alternate-handler-calls (atom []))
    (def alternate-processor
      (stream->batch
       (fn [batch] (swap! alternate-handler-calls conj batch))
       {:batch-size 3 :timeout 100 :threads 4}))

    (def slow-handler-calls (atom []))
    (def slow-processor
      (stream->batch
       (fn [batch] (Thread/sleep 100) (swap! slow-handler-calls conj batch))
       {:batch-size 5 :timeout 200 :threads 2})))

  (after
    (doseq [processor [message-processor
                       alternate-processor
                       slow-processor]]
    (future-cancel (deref (:timeout-handler (meta processor))))
    (purge-queue processor)
    (shutdown processor)))

  (it "should execute a batch when queue reaches batch-size"
    (doseq [x (range 20)]
      (message-processor x))
    (Thread/sleep 200)
    (should= 4 (count @handler-calls))
    (should= #{[0 1 2 3 4]
               [5 6 7 8 9]
               [10 11 12 13 14]
               [15 16 17 18 19]}
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
          tmp-proc (stream->batch
                    (fn [batch] (swap! tmp-handler-calls conj batch))
                    {:batch-size 5 :threads 2 :timeout 1000})]
      (doseq [x (range 15)]
        (tmp-proc x))
      (Thread/sleep 200)
      (should= 3 (count @tmp-handler-calls))
      (purge-queue tmp-proc)
      (future-cancel (deref (:timeout-handler (meta tmp-proc))))))

  (it "should return the queue size"
    (message-processor 666)
    (should= 1 (queue-size message-processor))
    (message-processor 777)
    (message-processor 888)
    (should= 3 (queue-size message-processor)))

  (it "should return the queue contents"
    (doseq [x ["a" "b" "c" "d"]]
      (message-processor x))
    (should= ["a" "b" "c" "d"]
             (sort (queue-contents message-processor))))

  (it "should purge the queue"
    (doseq [x ["a" "b" "c" "d"]]
      (message-processor x))
    (purge-queue message-processor)
    (should= [] (queue-contents message-processor))
    ;; and still be able to continue adding
    (doseq [x ["x" "y" "z"]]
      (message-processor x))
    (should= ["x" "y" "z"]
             (sort (queue-contents message-processor)))
    ;; and purge again
    (purge-queue message-processor)
    (should= [] (queue-contents message-processor))))
