from typing import Any, Dict, List, Optional

import pandas as pd
import requests
import snowflake.connector
import streamlit as st


FILE = "semantic_model.yaml"


st.title("Cortex Analyst")
st.markdown(f"Semantic Model: `{FILE}`")

