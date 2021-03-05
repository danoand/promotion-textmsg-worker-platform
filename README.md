### Summary

This repo is a set of web services that stand up "worker" jobs using the [Faktory](https://github.com/contribsys/faktory) worker queuing platform.

These related worker service comprise the "backend" of a text message based marketing application helping small business market and grow their business by tapping into the implicit network of their most valuable current customers.

### Services

Each web service represents a single purpose worker function.  Each service polls a cloud based Faktory job queue and triggers a job when work is queued by the main marketing application.  Those workers execute on these tasks:

* **Queue Job**: Enqueue a text message broadcast job reflecting a client promotion directed to the client's customer list
* **Fire SMS Messages**: Create the promotion text message, image artifact, and meta-data and decompose the broadcast into a collection of one or more sub-jobs
* **Generate QR Codes**: Generate unqiue QR codes for each individual texts that when scanned direct the viewer to additional product and promotion detail
* **Snapshot Process Data**: Take a data "snapshot" of the underlying promotion, product, recipient, and process data and save to the application NoSQL database for analytics and audit purposes
* **Job Halt Service**: Drain and/or halt remaining jobs if directed by an system administrator (if needed)