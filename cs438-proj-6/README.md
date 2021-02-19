# CS-438 Project: Hippocrates

## DISCLAIMER:
This work represents the joint effort of two fellow colleagues of mine and me. Please have a look at the report and slides to know more about our project.
The code does not represent truly our coding skills: there might be some weird things and design choices, mainly due to lack of time (tight schedule for submitting the project during exam session) and expertise. A code revision is definetely needed.

## Background:
Hippocrates is a decentralized, distributed, storage systems for medical records.
Three types of nodes exist in the network: cothority, doctors, patient.

## Patients:
Patients represent those user who can upload files.
## Doctors:
Doctors are those users who can retrieve files. Doctors should know their secret key (Cothority stores those in doctors.csv).
## Cothority:
Trusted, distributed, authority. Listens and serves patients and doctors request. They should be already up when patients or doctors are started.

## Usage:
Cothority nodes should be already before serving patients or doctors. See the example below. To start DKG protcol just input 'sc' after starting a Cothority node.

## Example:
./hw3 --role c --name C1 --gossipAddr 127.0.0.1:5000 --dhtAddr 127.0.0.1:4000 --cothorityAddr 127.0.0.1:5000,127.0.0.1:5001,127.0.0.1:5002 --dhtJoinAddr 127.0.0.1:4000
./hw3 --role c --name C2 --gossipAddr 127.0.0.1:5001 --dhtAddr 127.0.0.1:4001 --cothorityAddr 127.0.0.1:5000,127.0.0.1:5001,127.0.0.1:5002 --dhtJoinAddr 127.0.0.1:4000
./hw3 --role c --name C3 --gossipAddr 127.0.0.1:5002 --dhtAddr 127.0.0.1:4002 --cothorityAddr 127.0.0.1:5000,127.0.0.1:5001,127.0.0.1:5002 --dhtJoinAddr 127.0.0.1:4001
./hw3 --role d --name Dr.Cox --doctorKey hibarbie --gossipAddr 127.0.0.1:5003 --dhtAddr 127.0.0.1:4003 --cothorityAddr 127.0.0.1:5000,127.0.0.1:5001,127.0.0.1:5002 --dhtJoinAddr 127.0.0.1:4001
./hw3 --role p --name Dorian --gossipAddr 127.0.0.1:5004 --dhtAddr 127.0.0.1:4004 --cothorityAddr 127.0.0.1:5000,127.0.0.1:5001,127.0.0.1:5002 --dhtJoinAddr 127.0.0.1:4001


