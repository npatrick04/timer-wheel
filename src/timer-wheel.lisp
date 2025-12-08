;;;; timer-wheel.lisp

(in-package #:timer-wheel)

;;; "timer-wheel" goes here. Hacks and glory await!

(defparameter *default-resolution* 100 "milliseconds")
(defparameter *default-size* 100 "slots per wheel")

(define-condition unscheduled (error)
  ((timer :initarg :timer
	  :reader unscheduled-timer)))

(defclass timer ()
  ((remaining :accessor remaining
	      :initarg :remaining
	      :initform 'unscheduled)
   (installed-slot :accessor installed-slot
		   :initform nil)
   (callback :accessor callback
	     :initarg :callback)))

(defun make-timer (callback)
  "Return a timer object with CALLBACK being
a function that accepts WHEEL and TIMER arguments."
  (make-instance 'timer
		 :callback callback))

(defclass wheel ()
  ((context :accessor wheel-context
	    :initarg :context)
   (timeout-lock :accessor timeout-lock
		 :initform (bt:make-lock))
   (thread :accessor wheel-thread
	   :initarg :thread
	   :initform nil)
   (slots :accessor slots
	  :initarg :slots)
   (current-slot :accessor current-slot
		 :initform 0)
   (resolution :reader wheel-resolution
	       :initarg :resolution
	       :initform *default-resolution*)
   (reset :accessor reset
	  :initform nil)))

(defun make-wheel (&optional
		     (size *default-size*)
		     (resolution *default-resolution*)
		     (backend :bt))
  "Make a timer wheel with SIZE slots, with a millisecond RESOLUTION,
and BACKEND of :BT (bordeaux-threads... the only backend)."
  (make-instance 'wheel
		 :slots (make-array size :initial-element nil)
		 :resolution resolution
		 :context (ecase backend
			    (:bt (make-bt-context)))))

(defgeneric install-timer (wheel timer)
  (:documentation "Add TIMER to the WHEEL schedule."))
(defgeneric uninstall-timer (wheel timer)
  (:documentation "Remove TIMER from the WHEEL schedule."))
(defgeneric tick (wheel)
  (:documentation "Operate one tick of the wheel scedule."))

(defmethod tick ((wheel wheel))
  (let (tlist)
    ;; Update the wheel
    (bt:with-lock-held ((timeout-lock wheel))
      (setf (current-slot wheel) (mod (1+ (current-slot wheel))
				      (length (slots wheel))))
      (rotatef tlist (elt (slots wheel) (current-slot wheel))))

    ;; Then run the timers outside the lock
    (dolist (timer tlist)
      (let ((remaining (remaining timer)))
        (cond
          ((eq remaining 'unscheduled)
           ;; If timer was unsheduled during
           ;; the current  loop, then we need
           ;; to ignore such timer:
           nil)
          ((zerop (remaining timer))
           (funcall (callback timer) wheel timer))
          (t
           (install-timer wheel timer)))))))

(defmethod install-timer :before (wheel timer)
  (declare (ignore timer))
  (when (or (null (wheel-thread wheel))
            (not (bt:thread-alive-p
                  (wheel-thread wheel))))
    (initialize-timer-wheel wheel)))

(defun calculate-future-slot (current-slot remaining slots)
  (mod (+ current-slot
	  (max 1 ;minimum resolution of 1 tick
	       remaining ))
       slots))

(defmethod install-timer ((wheel wheel) (timer timer))
  (when (eq (remaining timer) 'unscheduled)
    (error 'unscheduled :timer timer))
  (let ((max-wheel-ticks (length (slots wheel))))
    (if (< (remaining timer) max-wheel-ticks)
	(bt:with-lock-held ((timeout-lock wheel))
	  (let ((future-slot (calculate-future-slot
			      (current-slot wheel)
			      (remaining timer)
			      (length (slots wheel)))))
	    (setf (remaining timer) 0
		  (installed-slot timer) future-slot

		  ;; Install the timer at the end of the list
		  ;; This ensures that timer evaluation order is FIFO.
		  ;; TODO: Actually use a FIFO instead of a list
		  (elt (slots wheel) future-slot)
		  (append (elt (slots wheel) future-slot)
			  (list timer)))
	    ;; (push timer (elt (slots wheel) future-slot))
	    ))
	;; The timer needs to be reinstalled later
	(progn (decf (remaining timer) max-wheel-ticks)
	       (push timer (elt (slots wheel) (current-slot wheel)))
	       (setf (installed-slot timer) (current-slot wheel))))
    t))

(defmethod uninstall-timer ((wheel wheel) (timer timer))
  (when (installed-slot timer)
    (bt:with-lock-held ((timeout-lock wheel))
      ;; Check again in case something else already uninstalled the timer.
      (when (installed-slot timer)
	(setf (elt (slots wheel) (installed-slot timer))
	      (remove timer
		      (elt (slots wheel) (installed-slot timer))
		      :test #'eq)
	      (remaining timer) 'unscheduled
	      (installed-slot timer) nil)))))

(defun schedule-timer (wheel timer &key
				     (ticks nil ticks-p)
				     (milliseconds nil milliseconds-p)
				     (seconds nil seconds-p))
  "Schedule a timer with one of
  :ticks - The number of resolution steps per (wheel-resolution wheel), minimum
           of 1 tick. 
  :milliseconds - The integer number of milliseconds worth of ticks, must be 
                  (and (plusp milliseconds) 
                       (zerop (mod milliseconds (wheel-resolution wheel)))).
  :seconds - The real value of seconds, rounded to the nearest resolution tick.

In all cases, the timeout will elapse in no more than 
   (* calculated-quantity-of-ticks resolution) milliseconds.

The keyword arguments are checked in this order: ticks, milliseconds, seconds
for valid values."
  (let ((calculated-timeout (cond
			      (ticks-p
			       ticks)
			      (milliseconds-p
			       (the integer (/ milliseconds
					       (wheel-resolution wheel))))
			      (seconds-p
			       (round (/ (* +milliseconds-per-second+ seconds)
					 (wheel-resolution wheel)))))))
    (assert (plusp calculated-timeout))
    (setf (remaining timer) calculated-timeout)
    (install-timer wheel timer)))

(defun initialize-timer-wheel (wheel)
  "Ensure the WHEEL is stopped, then initialize the WHEEL
context, and start the WHEEL thread."
  (shutdown-timer-wheel wheel)
  (initialize-timer (wheel-context wheel)
		    (wheel-resolution wheel))
  (setf (wheel-thread wheel)
	(bt:make-thread
	 (lambda ()
	   (manage-timer-wheel wheel))
	 :name "timer-wheel")))

(defun manage-timer-wheel (wheel)
  "This is the main entry point of the timer WHEEL thread."
  (loop
     (wait-for-timeout (wheel-context wheel))
     (when (reset wheel)
       (return))
     ;; Do the things!
     (tick wheel)))

(defun shutdown-timer-wheel (wheel)
  "Notify the wheel thread to terminate, then wait for it."
  (setf (reset wheel) t)

  (shutdown-context (wheel-context wheel))
  
  (when (wheel-thread wheel)
    (when (bt:thread-alive-p (wheel-thread wheel))
      (bt:join-thread (wheel-thread wheel)))
    (setf (wheel-thread wheel) nil))

  (setf (reset wheel) nil))


(defmacro with-timer-wheel (wheel &body body)
  "Execute BODY after initializing WHEEL, then
clean up by shutting WHEEL down after leaving the scope."
  `(unwind-protect
	(progn
	  (initialize-timer-wheel ,wheel)
	  ,@body)
     (shutdown-timer-wheel ,wheel)))
