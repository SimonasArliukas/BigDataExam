# AIS Vessel Collision Analyzer

This project is a PySpark-based data pipeline designed to process large-scale Automatic Identification System (AIS) ship tracking data. It filters, downsamples, and runs kinematic join algorithms to isolate, verify, and reconstruct marine vessel collision events. 

The system specifically targets asymmetric vessel encounters (e.g., Cargo vs. Tug/Workboat) utilizing parallelized geospatial geofencing and time-bucketing techniques.

## 🚀 Features
* **Big Data Processing:** Uses **Apache PySpark** to process hundreds of millions of tracking data rows concurrently.
* **Geospatial Geofencing:** Isolates vessel activities dynamically within specific coordinate boundaries and radius thresholds.
* **Kinematic Evaluation:** Flags dynamic heading shifts, course anomalies, and rapid vessel deceleration patterns.
* **Automated Visual Reconstruction:** Generates map trajectories and dynamic separation plots utilizing **Cartopy** and **Matplotlib**.

---

## 🛠️ Tech Stack & Dependencies
* **Core Language:** Python 3.10
* **Big Data Engines:** Apache PySpark 3.x & OpenJDK 21
* **Mapping & Visualization:** Cartopy (with C++ dependencies: `libgeos`, `libproj`), Matplotlib, Pandas, NumPy
* **Deployment Engine:** Docker & Docker Compose

---

## 📂 Project Structure
```text
BigDataExam/
├── data/                    # Put your raw .csv data files here (local only)
├── output/                  # Directory where trajectory charts are saved
├── Dockerfile               # Production multi-stage image definition
├── docker-compose.yml       # Orchestration layer configuration
├── incident_tracking.py     # Core PySpark execution pipeline script
├── requirements.txt         # Python application packages
└── README.md                # Documentation
