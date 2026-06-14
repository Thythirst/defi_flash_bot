"""
Chainlink Impact Simulator — production-grade pre-computation engine.

Detects imminent Chainlink oracle price updates and simulates their
impact on Aave V3 borrower health factors, identifying future
liquidation opportunities before they land on-chain.

Package structure:
    models.py    — SQLAlchemy ORM models (PostgreSQL)
    sync.py      — Redis → PostgreSQL state ETL
    simulator.py — Core simulation engine
    service.py   — Production daemon entry point
"""

__version__ = "1.0.0"
