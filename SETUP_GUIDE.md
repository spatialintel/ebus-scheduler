# eBus Scheduler — VS Code Setup Guide

## Step 1: Create the project folder

Open a terminal (or VS Code's built-in terminal) and run:

```bash
mkdir bus_scheduler
cd bus_scheduler
```

## Step 2: Download the files from Claude

Download the `bus_scheduler` folder from Claude's output. You'll get this structure:

```
bus_scheduler/
├── config/
│   └── eBus_Config_Input.xlsx    ← your input workbook
├── src/
│   ├── __init__.py
│   ├── models.py                 ← done
│   └── config_loader.py          ← done
├── tests/
│   ├── __init__.py
│   ├── test_models.py
│   └── test_config_loader.py
├── outputs/                      ← schedule output will go here
└── requirements.txt
```

Place these files exactly in this structure inside your `bus_scheduler/` folder.

## Step 3: Open in VS Code

```bash
code bus_scheduler/
```

Or: Open VS Code → File → Open Folder → select `bus_scheduler/`.

## Step 4: Set up Python environment

Open VS Code's terminal (Ctrl+` or Cmd+`):

```bash
# Create a virtual environment
python -m venv venv

# Activate it
# On Mac/Linux:
source venv/bin/activate
# On Windows:
venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

You should see the `(venv)` prefix in your terminal after activation.

In VS Code, press `Ctrl+Shift+P` → type "Python: Select Interpreter" → choose the one inside `venv/`.

## Step 5: Verify the setup

Run the tests from the terminal:

```bash
# Test the data models
python tests/test_models.py

# Test the config loader against the Excel file
python tests/test_config_loader.py
```

You should see "All tests passed." for both.

## Step 6: Edit your route data

1. Open `config/eBus_Config_Input.xlsx` in Excel or LibreOffice
2. Fill in your route's details (locations, fleet, distances, headway, travel times)
3. Save and close the file
4. **Important:** The Excel must be saved with calculated values (not just formulas).
   - In Excel: just save normally — values are stored automatically
   - In LibreOffice: save as `.xlsx`, it recalculates on save

## Step 7: Run the scheduler (once main.py is built)

After I build steps 3–7, you'll run:

```bash
python main.py config/eBus_Config_Input.xlsx
```

This will read your Excel, generate the schedule, and save the output to `outputs/R1_schedule.xlsx`.

---

## What's built so far

| File | Status | What it does |
|------|--------|-------------|
| `src/models.py` | Done | Trip, BusState, RouteConfig dataclasses |
| `src/config_loader.py` | Done | Reads Excel → returns RouteConfig + DataFrames |
| `src/distance_engine.py` | Next | OSRM auto-fetch if lat/lon provided |
| `src/trip_generator.py` | Pending | Generates all UP/DN departures + dead runs |
| `src/bus_scheduler.py` | Pending | Assigns buses, handles SOC + charging |
| `src/output_formatter.py` | Pending | Produces the final Excel output |
| `main.py` | Pending | Single entry point |

## What to do for each new route

1. Make a copy of `eBus_Config_Input.xlsx`
2. Rename it (e.g., `eBus_Config_R4.xlsx`)
3. Fill in that route's data
4. Run: `python main.py config/eBus_Config_R4.xlsx`
