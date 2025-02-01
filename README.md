# Otomoto Price Prediction - PAD I Final Project
## Magdalena Sokolowska
## Predicting used, no accident car price

## IMPORTANT Alternative: Download Preprocessed Data
If your PC is slow or you want to omit hours of scraping when generating `otomoto_cars.csv`, you can download the dataset from: \
[Google Drive Link](https://drive.google.com/file/d/1sGnE9eJetNg1TcoGLSfsMehVG4dB1yWF/view?usp=sharing)


## Overview
This repository contains code for scraping car data from Otomoto and predicting car prices based on the collected data. The project consists of:
- A web scraper to collect car listings from Otomoto.
- A Jupyter Notebook for data analysis and machine learning predictions.
- A requirements file listing the dependencies needed to run the project.

## Folder Structure
```
├── otomoto_scrapper.py  # Script for scraping Otomoto car listings
├── otomoto_car_price_prediction.ipynb  # Jupyter Notebook for data analysis and model training
├── requirements.txt  # List of required Python packages
```

## Prerequisites
- Python 3.8 or later
- Jupyter Notebook (for running the `.ipynb` file)
- Internet connection (for scraping and downloading dependencies)

## Installation
1. **Clone the repository:**
   ```sh
   git clone <repository_url>
   cd <repository_folder>
   ```

2. **Create and activate a virtual environment**
   ```sh
   python -m venv venv
   venv\Scripts\activate
   ```

3. **Install dependencies:**
   ```sh
   pip install -r requirements.txt
   ```

## Running the Scraper
If you want to collect fresh data from Otomoto:
```sh
python otomoto_scrapper.py
```
This will generate a `otomoto_cars.csv` file containing the scraped car listings.

## Running the Prediction Model
1. **Open Jupyter Notebook:**
   ```sh
   jupyter notebook
   ```
2. **Navigate to `otomoto_car_price_prediction.ipynb` and open it.**
3. **Run the cells in order to process the dataset and train the prediction model.**


## Troubleshooting
- **Scraper Issues:** The website structure might have changed/
  
## Contact
Magdalena Sokolowska 2025
