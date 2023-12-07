# The Genome Annotation System (GAS) 
# Key Functions:
- The GAS will allow a user to perform the following functions:
- Log in (via Globus Auth) to use the service -- Some aspects of the service are
available only to registered users. Two classes of users will be supported: Free and
Premium. Premium users will have access to additional functionality, beyond that
available to Free users. 
- Upgrade from a Free to a Premium user -- Premium users will be required to provide a
credit card for payment of the service subscription. Previously we would integrate with
Stripe for payment processing, but we’ve cut that due to the shortened project timeline.
- Submit an annotation job -- Free users may only submit jobs of up to a certain size.
Premium users may submit any size job. If a Free user submits an oversized job, the
system will refuse it and will prompt the user to convert to a Premium user.
- Receive notifications when annotation jobs finish -- When their annotation request is
complete, the GAS will send users an email that includes a link where they can view the
log file and download the results file.
- Browse jobs and download annotation results -- The GAS will store annotation
results for later retrieval. Users may view a list of their jobs (completed and running).
Free users may download results up to 10 minutes after their job has completed;
thereafter their results will be archived and only available to them if they convert to a
Premium user. Premium users will always have all their data available for download.
# System Components
- The GAS will comprise the following components:
- An object store for input files, annotated (result) files, and job log files.
- A key-value store for persisting information on annotation jobs.
- A low cost, highly-durable object store for archiving the data of Free users.
- A relational database for user account information.
- A service that runs AnnTools for annotation.
- A web application for users to interact with the GAS.
- A set of message queues and notification topics for coordinating system activity.
# GAS Scalability
We anticipate that the GAS will be in very high demand (since it’s a brilliant system developed
by brilliant students), and that demand will be variable over the course of any given time period.
Hence, the GAS will use elastic compute infrastructure to minimize cost during periods of low
demand and to meet expected user service levels during peak demand periods. We will build
elasticity into two areas of the GAS:
1. On the front end, the web application will be delivered by multiple servers running within
a load balancer. All requests will be received at a single domain name/IP address,
namely that of the load balancer. The load balancer will distribute requests across a pool
of identically configured, stateless, web servers running on EC2 instances. At minimum,
the load balancer will have two web server instances running across two availability
zones, providing capacity and ensuring availability in the event of failure. If demand on
either web server exceeds certain thresholds, the GAS will automatically launch
additional web servers and place them in the load balancer pool. When demand remains
below a certain threshold for a specified period of time, the GAS will terminate the
excess web servers.
2. On the back end, the annotator service will be delivered by multiple servers (optionally
running within a separate virtual private cloud). At minimum this pool of so-called “worker
nodes” will contain two nodes (EC2 instances). Additional instances will be launched and
added to (or removed from) the worker pool, based on the number of requests in the job
queue. The annotator servers store the state of running jobs locally -- in this sense they are not stateless like the web app servers.
If a job fails to complete it will leave the system in an inconsistent state, but it’s a state
from which we can recover relatively easily.


