(ns #^{:author "Edmund Jackson"}
  conduit.lbq
  (:use conduit.core)
  (:import (java.util.concurrent LinkedBlockingQueue)))


;; -----------------------------------------------------------------------------
;;  Queue operation functions

(defn create-queue []
  "Create a ref to an LBQ.  We use a ref because we want to use identity
semantics by using it as a key in the proc"
  (ref (LinkedBlockingQueue.)))

(defn enqueue [queue id message]
  "Put a message onto the queue, tagged with id.  This is used by external
functions to put data into a queue, as opposed to publish, below, which is
used internally."
  (.put @queue [id message]))

(defn- publish [queue message]
  "Put a message onto the queue."
  (.put @queue message))

(defn- consume [queue]
  "Take data off the head of the queue"
  (.take @queue))

;; -----------------------------------------------------------------------------
;; These are the three input functions for the lbq transport proc
(defn- lbq-pub-reply [source id]
  (fn lbq-reply [x]
    (let [reply-queue (create-queue)]
      (publish source [id [x reply-queue]])
      [(consume reply-queue) lbq-reply])))

(defn- lbq-pub-no-reply [source id]
  (fn lbq-no-reply [x]
    (publish source [id x])
    [[] lbq-no-reply]))

(defn- lbq-pub-sg-fn [source id]
  (fn lbq-reply [x]
    (let [reply-queue (create-queue)]
      (publish source [id [x reply-queue]])
      (fn []
        [(consume reply-queue) lbq-reply]))))

;; -----------------------------------------------------------------------------
;; Helper function for below
(defn- reply-fn [f]
  (partial (fn lbq-reply-fn [f [x reply-queue]]
             (let [[new-x new-f] (f x)]
               (publish reply-queue new-x)
               [[] (partial lbq-reply-fn new-f)]))
           f))

(defn a-lbq
  "Create an LBQ transported proc from a normal proc."
  [source id proc]
  (let [id (str id)
        reply-id (str id "-reply")]
    {:type :lbq
     :created-by (:created-by proc)
     :args (:args proc)
     :source source
     :id id
     :reply (lbq-pub-reply source reply-id)
     :no-reply (lbq-pub-no-reply source id)
     :scatter-gather (lbq-pub-sg-fn source reply-id)
     :parts (assoc (:parts proc)
              source {id (:no-reply proc)
                      reply-id (reply-fn (:reply proc))})}))

(defn lbq-entry [id proc]
  "Connects an existing proc onto an LBQ, hand back the conduit
 and a callback that feeds the LBQ's tail."
  (let [queue    (create-queue)
        callback (partial enqueue queue id)
        proc     (a-lbq queue id proc)]
    [callback proc]))

;; -----------------------------------------------------------------------------
(defn- msg-stream [queue]
  (fn this-fn [x]
    (try
      (let [msg (consume queue)]
        [[msg] this-fn])
      (catch InterruptedException e
        nil))))

(defn- msg-handler-fn [f msg]
  (try
    (let [new-f (second (f msg))]
      [[] (partial msg-handler-fn new-f)])
    (catch Exception e
      [[] f])))


(defn lbq-run [p queue]
  "Run the functions that read the LBQ and feed data into the proc."
  (when-let [handler-map (get-in p [:parts queue])]
    (let [select-handler (partial select-fn handler-map)
          handler-fn (comp-fn (msg-stream queue)
                              (partial msg-handler-fn
                                       select-handler))]
      (dorun (a-run handler-fn)))))

;; -----------------------------------------------------------------------------
