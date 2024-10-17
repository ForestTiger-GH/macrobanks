# -*- coding: utf-8 -*-

from setuptools import setup

setup(
   name='macrobanks',
   version='0.0.1',
   description='Module to deconstruct macroeconomic and bank statistic',
   author='Forest Tiger',
   packages= ['Банк России'],
   install_requires=['numpy', 'pandas'], #external packages as dependencies
)
