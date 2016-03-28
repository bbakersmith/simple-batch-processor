# Simple Batch Processor

A simple Clojure stream to batch processor. Allows a restricted number of
batch handler threads to be executed against a stream of incoming messages
on both batch-size and timeout.

## Usage

### Stream->Batch Function

```
(def message-processor 
  (stream->batch
    (fn [batch] (do-some-things batch))
    {:batch-size 5 :timeout 100 :threads 4}))

(doseq [x (range 9001)]
  (message-processor x))
```

### Def Var

```
(defstream->batch message-processor
  (fn [batch] (do-some-things batch))
  {:batch-size 5 :timeout 100 :threads 4})

(doseq [x (range 9001)]
  (message-processor x))
```

### Temporary Scoped Binding

```
(with-stream->batch [tmp-proc (fn [batch] (do-some-things batch))
                     {:batch-size 5 :threads 2 :timeout 1000}]
  (doseq [x (range 17)]
    (tmp-proc x)))
```

## License

Simple Batch Processor is released under the Apache License.
See LICENSE.txt(LICENSE.txt) for complete license text.

Copyright [2016] [Benjamin Baker-Smith]
