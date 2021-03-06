;;;; timer-wheel.asd

(asdf:defsystem #:timer-wheel
  :description "A timer wheel implementation with BORDEAUX-THREADS backend."
  :author "Nick Patrick <npatrick04@gmail.com>"
  :license "MIT"
  :depends-on (#:bordeaux-threads)
  :serial t
  :components ((:module "src"
			:components
			((:file "package")
			 (:file "utils")
			 (:file "bt-timeout")
			 (:file "timer-wheel")))))

