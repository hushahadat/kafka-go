# Kafka + Go Setup (KRaft Mode)

This repository contains a minimal local setup of **Apache Kafka (KRaft mode)** using **Docker** and a simple **Go producer** to publish messages into a Kafka topic.

---

## 🚀 Prerequisites

- **Go** installed  
- **Docker Desktop** installed  
- **Windows / macOS / Linux** supported  

---

## 📦 Folder Structure
├── docker-compose.yml
├── producer.go
└── README.md

---

## 🐳 Start Kafka (KRaft Mode)

```sh
docker compose up -d
docker ps 
