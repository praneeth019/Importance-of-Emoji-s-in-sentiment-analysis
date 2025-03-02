# Exploring importance of emoji's in semtiment analysis

A machine learning and NLP project for sentiment analysis of tweets using transformers-based models. This project implements and compares two approaches:

1. A DistilBERT-based model for basic sentiment classification
2. A custom RoBERTa-based model that processes both tweet text and emoticons separately 

Both of them are compared to identify the effect of emoticons.

## Project Overview

This project aims to classify tweets as either positive or negative using state-of-the-art transformer models. The implementation includes data preprocessing, model training, and evaluation. Two distinct approaches are provided:

- **DistilBERT Approach**: A lightweight transformer model that processes the entire tweet as a single input
- **Custom RoBERTa Approach**: An innovative dual-input approach that processes tweet text and emoticons separately before combining their representations

## Requirements

- Python 3.7+
- PyTorch
- Transformers (Hugging Face)
- Pandas
- Scikit-learn
- Jupyter Notebook

## Installation

```bash
# Clone this repository
git clone https://github.com/praneethr0019/importance-of-emoji-s-in-sentiment-analysis.git
cd importance-of-emoji-s-in-sentiment-analysis

# Create and activate a virtual environment (recommended)
python -m venv venv
source venv/bin/activate  # On Windows, use: venv\Scripts\activate

# Install required packages
pip install torch pandas scikit-learn transformers jupyter
```

## Dataset

The project uses a Twitter sentiment dataset with the following characteristics:
- Binary sentiment classification (0 for negative, 1 for positive)
- Contains tweet text and emoticons
- The original dataset has been preprocessed to standardize labels (changing 4 to 1)

Place your dataset in the project directory and update the file path in the notebooks:
- For DistilBERT approach: Update `path = 'path to data'` in `distilbert_noemoji.ipynb`
- For RoBERTa approach: Update `data_path = '/content/drive/MyDrive/datasets/orig.csv'` in `twitter_roberta.ipynb`

## Model Approaches

### 1. DistilBERT Approach (`distilbert_noemoji.ipynb`)

This notebook implements a simple sentiment classifier using DistilBERT:
- Uses Hugging Face's `DistilBertForSequenceClassification`
- Processes the entire tweet as a single input
- Implements custom callbacks for training monitoring
- Provides comprehensive evaluation using classification metrics

### 2. Custom RoBERTa Approach (`twitter_roberta.ipynb`)

This notebook implements a more advanced dual-input model:
- Uses `RobertaModel` with a custom classification head
- Processes tweet text and emoticons through separate RoBERTa encoders
- Concatenates the encoded representations before classification
- Implements a custom PyTorch training loop

## Usage

1. Open the desired Jupyter notebook:
   ```bash
   jupyter notebook distilbert_noemoji.ipynb
   # or
   jupyter notebook twitter_roberta.ipynb
   ```

2. Update the data path in the notebook to point to your dataset

3. Run all cells to:
   - Load and preprocess the data
   - Create and train the model
   - Evaluate performance on test data

## Results

Both models are evaluated using standard classification metrics:
- Accuracy
- Precision
- Recall
- F1 Score

The custom RoBERTa model with separate emoticon processing is designed to better capture the sentiment conveyed through emoticons, potentially improving performance on tweets with heavy emoji usage.

## Customization

You can customize several aspects of the models:
- Training epochs
- Batch size
- Learning rate
- Model architecture (by modifying the custom classifier for RoBERTa)
- Text preprocessing steps

## Acknowledgments

- The project uses models from the Hugging Face Transformers library
- The RoBERTa model specifically uses the Twitter-optimized version from Cardiff NLP
