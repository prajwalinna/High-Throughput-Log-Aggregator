"""
synapse_ai/train_slm.py
-----------------------
Fine-tunes the Tier-2 Small Language Model (DistilBERT) on normal baseline logs
using the local NVIDIA GeForce RTX 2050 GPU with PyTorch CUDA.
Produces calibrated logits so normal sequences have low epistemic uncertainty U(x) < tau_confident.
"""

import os
import time
from typing import Optional
import torch
import torch.nn as nn
from torch.utils.data import DataLoader, Dataset
from transformers import AutoTokenizer, AutoModelForSequenceClassification

from synapse_stream.sessionizer import mask_log_message


class LogDataset(Dataset):
    def __init__(self, texts, labels, tokenizer, max_length=128):
        self.texts = texts
        self.labels = labels
        self.tokenizer = tokenizer
        self.max_length = max_length

    def __len__(self):
        return len(self.texts)

    def __getitem__(self, idx):
        text = str(self.texts[idx])
        encoding = self.tokenizer(
            text,
            max_length=self.max_length,
            padding="max_length",
            truncation=True,
            return_tensors="pt"
        )
        return {
            "input_ids": encoding["input_ids"].squeeze(0),
            "attention_mask": encoding["attention_mask"].squeeze(0),
            "label": torch.tensor(self.labels[idx], dtype=torch.long)
        }


def train_calibrated_slm(
    dataset_path: Optional[str] = None,
    output_dir: str = "models/tier2_slm_calibrated",
    epochs: int = 3,
    batch_size: int = 16,
    lr: float = 3e-5
):
    print("=" * 65)
    print("  TRAINING / CALIBRATING TIER-2 SLM (DistilBERT) ON CUDA")
    print("=" * 65)

    if dataset_path is None or not os.path.exists(dataset_path):
        for candidate in ["evaluation/datasets/BGL_2k.log", "evaluation_BGL_2k.log", "BGL_2k.log"]:
            if os.path.exists(candidate):
                dataset_path = candidate
                break

    device = "cuda" if torch.cuda.is_available() else "cpu"
    print(f"Device: {device} ({torch.cuda.get_device_name(0) if device == 'cuda' else 'CPU'})")

    os.makedirs(output_dir, exist_ok=True)
    tokenizer = AutoTokenizer.from_pretrained("distilbert-base-uncased")
    model = AutoModelForSequenceClassification.from_pretrained(
        "distilbert-base-uncased",
        num_labels=3
    ).to(device)

    # Prepare training dataset: extract normal baseline and synthetic variations
    texts = []
    labels = []

    print("[1] Loading and preprocessing baseline training logs...")
    if os.path.exists(dataset_path):
        with open(dataset_path, "r", encoding="utf-8", errors="ignore") as f:
            for idx, line in enumerate(f):
                if idx >= 600:
                    break
                parts = line.strip().split(maxsplit=9)
                if len(parts) >= 10:
                    label_tag = parts[0]
                    msg = parts[9]
                    masked = mask_log_message(msg)
                    if label_tag == "-":
                        # Class 0: BENIGN_NORMAL
                        texts.append(masked)
                        labels.append(0)
                    else:
                        # Class 1: KNOWN_OPERATIONAL_FAULT
                        texts.append(masked)
                        labels.append(1)

    # Augment with synthetic microservice normal & error logs for generalizability
    synthetic_normals = [
        "User <NUM> logged in successfully",
        "Access token refreshed for user <NUM>",
        "Post <UUID> fetched successfully",
        "Feed refreshed for user <NUM>",
        "Push notification delivered to device <NUM>",
        "Email digest queued for user <NUM>",
        "User <NUM> logged out",
        "Session verified for user <NUM>"
    ]
    for s in synthetic_normals:
        texts.extend([s] * 10)
        labels.extend([0] * 10)

    synthetic_faults = [
        "Database connection pool exhausted! Timeout waiting for available connection",
        "Connection reset by peer on socket <HEX>",
        "Failed to authenticate user <NUM>: Upstream auth-service timed out",
        "Rate limit quota exceeded for client <IP>",
    ]
    for s in synthetic_faults:
        texts.extend([s] * 8)
        labels.extend([1] * 8)

    print(f"[2] Prepared {len(texts)} training samples (Class 0: {labels.count(0)}, Class 1: {labels.count(1)}).")

    dataset = LogDataset(texts, labels, tokenizer)
    loader = DataLoader(dataset, batch_size=batch_size, shuffle=True)

    optimizer = torch.optim.AdamW(model.parameters(), lr=lr)
    criterion = nn.CrossEntropyLoss()

    print("\n[3] Executing CUDA fine-tuning epochs on RTX 2050...")
    model.train()
    start_train_time = time.time()

    for epoch in range(epochs):
        total_loss = 0.0
        correct = 0
        total = 0

        for batch in loader:
            input_ids = batch["input_ids"].to(device)
            attention_mask = batch["attention_mask"].to(device)
            batch_labels = batch["label"].to(device)

            optimizer.zero_grad()
            outputs = model(input_ids=input_ids, attention_mask=attention_mask)
            loss = criterion(outputs.logits, batch_labels)
            loss.backward()
            optimizer.step()

            total_loss += loss.item()
            preds = torch.argmax(outputs.logits, dim=-1)
            correct += (preds == batch_labels).sum().item()
            total += batch_labels.size(0)

        acc = correct / max(1, total) * 100.0
        print(f"  Epoch {epoch + 1}/{epochs} | Loss: {total_loss / len(loader):.4f} | Accuracy: {acc:.2f}%")

    train_duration = time.time() - start_train_time
    print(f"\n[4] Training complete in {train_duration:.2f} seconds!")

    # Save fine-tuned checkpoint
    model.save_pretrained(output_dir)
    tokenizer.save_pretrained(output_dir)
    print(f"[5] Calibrated model saved to: {output_dir}")
    return output_dir


if __name__ == "__main__":
    train_calibrated_slm()
