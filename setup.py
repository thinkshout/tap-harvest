#!/usr/bin/env python

from setuptools import setup

setup(name='tap-harvest',
      version="2.0.7",
      description='Singer.io tap for extracting data from the Harvest api',
      author='Facet Interactive',
      url='http://singer.io',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_harvest'],
      install_requires=[
          'singer-python==5.9.0',
          'requests==2.21.0',
          'pendulum==1.2.0'
      ],
      entry_points='''
          [console_scripts]
          tap-harvest=tap_harvest:main
      ''',
      packages=['tap_harvest'],
      package_data = {
          'tap_harvest/schemas': [
              "clients.json",
              "contacts.json",
              "estimate_item_categories.json",
              "estimates.json",
              "expense_categories.json",
              "expenses.json",
              "invoice_item_categories.json",
              "invoices.json",
              "projects.json",
              "roles.json",
              "task_assignments.json",
              "tasks.json",
              "time_entries.json",
              "user_assignments.json",
              "users.json",
          ],
      },
      include_package_data=True,
)
