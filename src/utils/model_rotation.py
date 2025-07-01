#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import threading
from typing import List


class ModelRotation:
    """Thread-safe round-robin model rotation"""
    
    def __init__(self):
        self._stage1_counter = 0
        self._stage2_counter = 0
        self._lock = threading.Lock()
    
    def get_next_stage1_model(self, models: List[str]) -> str:
        """Get next stage1 model using round-robin"""
        with self._lock:
            index = self._stage1_counter % len(models)
            self._stage1_counter += 1
            return models[index]
    
    def get_next_stage2_model(self, models: List[str]) -> str:
        """Get next stage2 model using round-robin"""
        with self._lock:
            index = self._stage2_counter % len(models)
            self._stage2_counter += 1
            return models[index]


# Global instance
_model_rotation = ModelRotation()


def get_stage1_model(models: List[str]) -> str:
    """Get next stage1 model"""
    return _model_rotation.get_next_stage1_model(models)


def get_stage2_model(models: List[str]) -> str:
    """Get next stage2 model"""
    return _model_rotation.get_next_stage2_model(models)


if __name__ == "__main__":
    print("=== Model Rotation Test ===\n")
    
    stage1_models = [
        "gemini-2.5-flash",
        "gemini-2.5-flash-preview-04-17", 
        "gemini-2.5-flash-preview-05-20"
    ]
    
    print("Stage1 rotation:")
    for i in range(8):
        model = get_stage1_model(stage1_models)
        print(f"  Request {i+1}: {model}")
    
    print("\n✅ Simple round-robin working!")