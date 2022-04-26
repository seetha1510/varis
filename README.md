# README

## Introduction ##

Our team is creating metrics and developing an accuracy algorithm for Varis. These metrics and the algorithm will be used to determine how well classified the items withing the Varis taxonomy are. A score will be assigned at each level and will all be stored in a .csv file. Using these scores, Varis can then classify these products correctly and avoid any condfusion on their e-commerce websites well ahead of time.

## Requirements ##

This code has been run and tested on:

* Python - 3.7.0
* Yake - 0.4.8
* Sentence_transformers - 2.2.0
* Rake-nltk - 1.0.6


## External Deps  ##

* Git - Downloat latest version at https://git-scm.com/book/en/v2/Getting-Started-Installing-Git
* AWS Sagemaker -  https://aws.amazon.com/sagemaker


## Tests ##

Unit as well as integration tests have been conducted locally but aren't part of the final code base.


## Execute Code ##

Log into the AWS SageMaker Studio - https://aws.amazon.com/sagemaker

Open the FINAL_CODE/FINAL_CODE_v1 folder.

Open makefile.ipnyb and run it from the very top.

This will install all the dependencies for the roject to run and will also run the file.

## Deployment ##

No Deployment required for this project.


## CI/CD ##

No CI/CD was implemented as most of the code was worked on SageMaker 

## Support ##

The support for this project has been officially closed. The customer was satisfied with the product and there's nothing else we could do besides improving scalability. As of right now there are no future goals for this project.