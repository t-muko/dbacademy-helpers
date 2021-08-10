# Databricks notebook source
import setuptools
from setuptools import find_packages

setup_var = "moo"

setuptools.setup(
    name="user-setup",
    version="1.0",
    packages=find_packages(),
)
