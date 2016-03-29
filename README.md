# Simple Batch Processor

A simple Clojure stream to batch processor. Allows a restricted number of
batch handler threads to be executed against a stream of incoming messages
on both batch-size and timeout.

Simple Batch Processor may be installed from the Clojars repository.
```clojure
[bbakersmith/simple-batch-processor "2.0.0"]
```

## Usage

### Create Batch Processor Functions

#### stream->batch

Returns a batch processing function.

If you want to dispose of the associated threadpool, you must manually
call `shutdown`.

```clojure
(require '[simple-batch-processor.core :refer [stream->batch]])

(def message-processor
  (stream->batch
    (fn [batch] (do-some-things batch))
    {:batch-size 5 :timeout 100 :threads 4}))

(doseq [x (range 9001)]
  (message-processor x))
```

### Utility Functions

#### queue-size
#### queue-contents
#### purge-queue
#### shutdown

## Design

![Simple Batch Processor Diagram](/doc/simple-batch-processor-diagram.png)

## License

Simple Batch Processor is released under the Apache License.
See [LICENSE.txt](LICENSE.txt) for complete license text.

Copyright [2016] [Benjamin Baker-Smith]
