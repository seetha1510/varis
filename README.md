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

* Git - Download latest version at https://git-scm.com/book/en/v2/Getting-Started-Installing-Git
* AWS Sagemaker -  https://aws.amazon.com/sagemaker


## Tests ##

Unit as well as integration tests have been conducted locally but aren't part of the final code base.


## Execute Code ##

### Using Juptyer notebook ###
Run all cells in the makefile.ipynb. The first few cells will download required modules. The last cell requires three inputs: the datafile name (as a .csv), name of database that is going to be created, name of output file (as a .csv)

### Using python ###
Download the required modules mentioned in requirements. The cells in the Juptyer notebook detail this. Once downloaded run the run.py with command line inputs: the datafile name (as a .csv), name of database that is going to be created, name of output file (as a .csv)


## Deployment ##

No Deployment required for this project.


## CI/CD ##

No CI/CD was implemented as most of the code was worked on SageMaker 

## Support ##

The support for this project has been officially closed. The customer was satisfied with the product and there's nothing else we could do besides improving scalability. As of right now there are no future goals for this project.
