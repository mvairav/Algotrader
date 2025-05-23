import time
from datetime import datetime
import collections
import random # For simulating data updates
from typing import Dict, Any, Deque, List
import asyncio
from rich.console import Console
from rich.layout import Layout
from rich.panel import Panel
from rich.table import Table
from rich.live import Live
from rich.text import Text
from rich.columns import Columns

# Define a type for contract data for clarity
ContractDetails = Dict[str, Any] # Stores strike, close_price_contract, bot, sold, pnl_contract
ContractLiveData = collections.deque # Stores the live feed values (max 5)
ContractData = Dict[str, Any] # A contract's full data, including details and live_feed deque
AllContractsData = Dict[str, ContractData] # All contracts, keyed by symbol

# --- Global State (simulated) ---
# In a real app, this would come from your data source
current_contracts_data: AllContractsData = {}
current_global_close_price: float = 0.0
current_global_total_pnl: float = 0.0
static_date_str = datetime.now().strftime("%Y-%m-%d") # Set date once at the start

def initialize_data(
    initial_contracts_data: AllContractsData,
    initial_global_close_price: float,
    initial_global_total_pnl: float
):
    """Initializes the global data variables."""
    global current_contracts_data, current_global_close_price, current_global_total_pnl
    
    current_global_close_price = initial_global_close_price
    current_global_total_pnl = initial_global_total_pnl
    current_contracts_data = {} # Clear previous data

    for symbol, data in initial_contracts_data.items():
        # Ensure each contract has a live_feed deque
        live_feed_dq = data.get("live_feed")
        if not isinstance(live_feed_dq, collections.deque):
            # If it's a list or None, convert/create a deque
            initial_feed_list = list(live_feed_dq) if live_feed_dq else []
            live_feed_dq = collections.deque(initial_feed_list, maxlen=5)
        
        current_contracts_data[symbol] = {
            "details": {
                "strike": data.get("strike", 0.0),
                "close_price_contract": data.get("close_price_contract", 0.0),
                "bot": data.get("bot", 0),
                "sold": data.get("sold", 0),
                "pnl_contract": data.get("pnl_contract", 0.0),
            },
            "live_feed": live_feed_dq
        }
        # Pre-populate with a few dummy values if empty for better initial visual
        if not current_contracts_data[symbol]["live_feed"]:
            for i in range(random.randint(1, 3)):
                current_contracts_data[symbol]["live_feed"].append(
                    f"{random.uniform(data.get('close_price_contract', 50) - 5, data.get('close_price_contract', 50) + 5) + i:.2f}"
                )


def generate_layout() -> Layout:
    """Generates the entire console layout with current data."""
    root_layout = Layout(name="root")

    root_layout.split_column(
        Layout(name="top_info", size=4),      # Adjusted size for 2 lines + panel borders/title
        Layout(name="contracts_area")        # For contract boxes
    )

    # --- Top Info Section (Date, Time, Global Stats) ---
    current_time_str = datetime.now().strftime("%H:%M:%S")
    
    top_info_table = Table.grid(expand=True, padding=(0, 1))
    top_info_table.add_column(justify="left", ratio=1)  # For PnL and Close Price
    top_info_table.add_column(justify="right", ratio=1) # For Date and Time

    top_info_table.add_row(
        f"Overall Close Price: [bold green]{current_global_close_price:.1f}[/]",
        f"Date: {static_date_str}"
    )
    top_info_table.add_row(
        f"Overall Total PnL: [bold green]{current_global_total_pnl:,.0f}[/]",
        f"Time: [bold yellow]{current_time_str}[/]"
    )
    
    root_layout["top_info"].update(
        Panel(
            top_info_table, 
            title="[bold cyan]Market Overview[/]", 
            border_style="cyan"
        )
    )

    # --- Contracts Area Section ---
    contract_panels: List[Panel] = []
    # Sort keys for consistent order of display
    sorted_contract_symbols = sorted(current_contracts_data.keys())

    for symbol in sorted_contract_symbols:
        contract_full_data = current_contracts_data[symbol]
        contract_details = contract_full_data["details"]
        live_feed_dq = contract_full_data["live_feed"]
        
        # Column 1: Contract Details
        details_table = Table.grid(padding=(0, 1)) 
        details_table.add_column(style="cyan", no_wrap=True) 
        details_table.add_column(justify="right", style="white")       

        details_table.add_row("Strike:", f"{contract_details.get('strike', 0.0):.1f}")
        details_table.add_row("Close:", f"{contract_details.get('close_price_contract', 0.0):.1f}")
        details_table.add_row("Bot:", str(contract_details.get("bot", 0)))
        details_table.add_row("Sold:", str(contract_details.get("sold", 0)))
        
        pnl_value = contract_details.get('pnl_contract', 0.0)
        pnl_style = "green" if pnl_value >= 0 else "red"
        details_table.add_row("PnL:", f"[{pnl_style}]{pnl_value:,.1f}[/{pnl_style}]")


        # Column 2: Live Feed Values
        live_feed_texts: List[Text] = []
        for i, val_str in enumerate(list(live_feed_dq)):
            # Highlight the latest value (last one in deque)
            if i == len(live_feed_dq) -1:
                 live_feed_texts.append(Text(val_str, style="bold yellow"))
            else:
                 live_feed_texts.append(Text(val_str, style="dim"))
        live_feed_renderable = Text("\n").join(live_feed_texts)
        
        # Create a table to hold the two columns side-by-side within the panel
        inner_panel_table = Table.grid(expand=True)
        inner_panel_table.add_column(ratio=2, min_width=20) # Details column
        inner_panel_table.add_column(ratio=1, justify="right", min_width=10) # Live feed column
        inner_panel_table.add_row(details_table, live_feed_renderable)

        contract_panel = Panel(
            inner_panel_table,
            title=f"[bold blue]{symbol}[/]",
            border_style="blue",
            expand=False 
        )
        contract_panels.append(contract_panel)
    
    if not contract_panels:
        root_layout["contracts_area"].update(
            Panel(Text("No contracts to display.", justify="center"), 
                  title="[dim]Contracts[/dim]", 
                  border_style="dim", 
                  height=10) # Give it some height
        )
    else:
        # Using Columns to display panels side-by-side
        # `equal=True` tries to make columns of equal width
        # `expand=True` allows Columns to take available space
        root_layout["contracts_area"].update(
            Columns(contract_panels, expand=True, equal=False, align="center", padding=1)
        )
        
    return root_layout

async def run_data_simulation_loop():
    """Runs the main data simulation loop, updating global variables."""
    global current_contracts_data, current_global_total_pnl, current_global_close_price
    loop_count = 0
    try:
        while True:
            await asyncio.sleep(0.5) # Simulation tick rate (data updates)
            loop_count += 1

            # Update global stats every few ticks
            if loop_count % 10 == 0:  # e.g., every 5 seconds (0.5s * 10)
                current_global_total_pnl += random.randint(-25, 30) * 10
                current_global_close_price += random.uniform(-0.5, 0.5)
                current_global_close_price = round(current_global_close_price, 1)

            # Update existing contracts
            for symbol in list(current_contracts_data.keys()): # list() for safe iteration if removing
                if symbol in current_contracts_data: 
                    contract_data = current_contracts_data[symbol]
                    details = contract_data["details"]
                    
                    details["pnl_contract"] = round(
                        details.get("pnl_contract", 0.0) + random.uniform(-15, 18), 1
                    )
                    
                    new_feed_val = f"{random.uniform(details.get('close_price_contract',50)-2, details.get('close_price_contract',50)+2):.2f}"
                    contract_data["live_feed"].append(new_feed_val)
                    
                    if random.random() < 0.1: # 10% chance to update bot/sold
                        details["bot"] = details.get("bot",0) + random.randint(0,2)
                    if random.random() < 0.05: # 5% chance
                        details["sold"] = details.get("sold",0) + random.randint(0,1)

            # Simulate adding a new contract
            if loop_count > 0 and loop_count % 30 == 0: # e.g., every 15 seconds
                new_strike_base = random.choice([4000, 5000, 18000, 19000])
                new_symbol = f"NEW{random.randint(100, 999)} {'C' if random.random() > 0.5 else 'P'}{new_strike_base + random.randint(-2,2)*50}"
                if new_symbol not in current_contracts_data:
                    new_close_price = round(random.uniform(20, 200),1)
                    current_contracts_data[new_symbol] = {
                        "details": {
                            "strike": round(random.uniform(new_strike_base-100, new_strike_base+100),1),
                            "close_price_contract": new_close_price,
                            "bot": random.randint(0, 30),
                            "sold": random.randint(0, 20),
                            "pnl_contract": round(random.uniform(-50, 50),1),
                        },
                        "live_feed": collections.deque(
                            [f"{random.uniform(new_close_price-5, new_close_price+5):.2f}" for _ in range(random.randint(1,3))], 
                            maxlen=5
                        )
                    }
            
            # Simulate removing a contract (less frequently)
            # Ensure there's more than one contract before removing
            if loop_count > 0 and loop_count % 70 == 0 and len(current_contracts_data) > 1:
                symbol_to_remove = random.choice(list(current_contracts_data.keys()))
                # Make sure not to remove the only contract if we want to always show at least one (if any exist)
                if len(current_contracts_data) > 1 : # Double check, could be modified by another thread in real app
                     del current_contracts_data[symbol_to_remove]
    except KeyboardInterrupt:
        # This allows the simulation loop to be interrupted cleanly if run directly,
        # but the main KeyboardInterrupt handling for the Live display is in display_rich_console_info_main_loop
        pass


async def display_rich_console_info_main_loop():
    """
    Main function to set up and run the live display.
    Data simulation runs in a separate function.
    """
    task = run_data_simulation_loop()
    await asyncio.sleep(2)  # Give some time for the simulation to start
    console = Console()
    live = Live(generate_layout)  # Create a Live object
    live.console = console  # Set the console for the Live object
    live.refresh_per_second = 2  # Set refresh rate
    live.screen = False  # Disable screen clearing
    live.transient = False  # Keep the display on screen
    live.get_renderable = generate_layout  # Set the function to generate the layout
    live.start(refresh=True)  # Start the live display
    await task
    live.stop()  # Stop the live display when done

if __name__ == "__main__":
    # Example initial data
    sample_contracts: AllContractsData = {
        "ESZ25 C4850": { # Changed year and strike for variety
            "strike": 4850.0,
            "close_price_contract": 112.5,
            "bot": 80,
            "sold": 72,
            "pnl_contract": 150.0,
            "live_feed": collections.deque(["112.50", "112.55", "112.40", "112.60"], maxlen=5)
        },
        "NQU25 P19200": { # Changed year and strike
            "strike": 19200.0,
            "close_price_contract": 75.8,
            "bot": 30,
            "sold": 35,
            "pnl_contract": -80.5,
            "live_feed": ["75.80", "75.75"] # Will be converted to deque
        },
         "RTYM25 C2100": { # Added a third contract
            "strike": 2100.0,
            "close_price_contract": 45.0,
            "bot": 15,
            "sold": 10,
            "pnl_contract": 25.0,
            # live_feed will be initialized by the function if empty or missing
        }
    }
    sample_global_close: float = 2050.5
    sample_global_pnl: float = 75000.0

    initialize_data(sample_contracts, sample_global_close, sample_global_pnl)
    asyncio.run(display_rich_console_info_main_loop())
