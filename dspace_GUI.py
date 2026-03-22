import tkinter as tk
from tkinter import ttk
from tkinter import messagebox
from tkinter import filedialog
from datetime import datetime
import os
import threading
import json
import re
import multiprocessing
import pywintypes
import shutil
import warnings
from PIL import Image, ImageTk

# Suppress SSL verification warnings (HiL PCs use internal IPs)
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Import the conversion function
# Files been used related to this GUI
#ControlDesk.py, ConvertCSV2BLF_ForTwoCANs.py
try:
    # Assuming the optimized conversion script is now named ConvertCSV2BLF_ForTwoCANs.py
    from ConvertCSV2BLF_ForTwoCANs import write_blf, upload_blf_to_gcs
    CONVERTER_AVAILABLE = True
except ImportError as e:
    CONVERTER_AVAILABLE = False
    print(f"Warning: BLF Converter functionality unavailable - {e}")

def _upload_blf_noop(*args, **kwargs):
    print("upload_blf_to_gcs not available")
    return False

if not CONVERTER_AVAILABLE:
    upload_blf_to_gcs = _upload_blf_noop

# Import the existing ControlDesk module
try:
    from ControlDesk import dSpace_ControlDesk as ControlDesk
    CONTROLDESK_AVAILABLE = True
except ImportError:
    CONTROLDESK_AVAILABLE = False

# Import CAN Streaming module
try:
    from CanStreaming import CanStreamManager, BUS_API_AVAILABLE, PUBSUB_AVAILABLE
    CAN_STREAMING_AVAILABLE = True
except ImportError:
    CAN_STREAMING_AVAILABLE = False
    BUS_API_AVAILABLE = False
    PUBSUB_AVAILABLE = False

class DSpaceGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("SBTL dSpace Application")
        self.root.geometry("900x800")
        self.root.attributes('-topmost', True)
        
        # Define log file path and settings file location
        self.log_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), "dspace_gui.log")
        self.settings_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), "dspace_settings.json")
        try:
            self.logicon=os.path.join(os.path.dirname(os.path.abspath(__file__)), "ftd.ico")
            if os.path.exists(self.logicon):
                self.root.iconbitmap(self.logicon)
            else:
                 print("Icon file 'ftd.ico' not found.")
        except Exception as icon_error:
             print(f"Error setting icon: {icon_error}")
        
        # Define colors for readonly and normal combobox states
        self.readonly_bg = '#e0e0e0'  # Lighter gray for disabled state
        self.normal_bg = 'white'      # Normal white background
        
        # Add a safe logging method for initialization phase
        self.log_area = None
        
        # Initialize default settings - add paths for converter
        self.settings = {
            'batpack_id': 'TestPack001',
            'cdp_path': r"C:\00_BTF_Projects\01_Scalexio_Projects\03_BTF_DS_EMA_Prj\Instrumentation\CLD_Prj_EMA_IPB2_NCR24Q2_v24\CLD_Prj_EMA_IPB2_NCR24Q2_v24.CDP",
            'experiment_name': 'BTF_Experiment',
            'controldesk_visible': True,
            'auto_record': False,
            'blf_pmz_csv_path': '', # Add default for PMZ CSV
            'blf_debug_csv_path': '', # Add default for Debug CSV
            'blf_sdu_csv_path': '', #FOR 3-CAN MODE
            'can_mode': '2-CAN', #FOR 3-CAN MODE
            # GCS upload settings for BLF integration
            'test_name': '',
            'ddg_environment': 'prod',
            'test_setup': '',
            'sample_no': '',
            'backend_api_url': 'https://gemini-dash.jlr-apps.com/api',
            'api_token': '03578a8686b5fc6007a6e5266f841756bcd58541',
            'auto_upload_blf': True,
            'blf_save_interval': 60,
            # CAN Streaming settings
            'stream_rig_id': 'vib1_horizontal',
            'stream_scalexio_ip': '192.168.0.10',
            'stream_can_fd': False,
            'stream_pubsub_project': 'jlr-eng-ftd-tool-prod',
            'stream_pubsub_topic': 'sbtl-can-stream',
        }
        
        # Load settings early - we'll apply them to UI elements as they are created
        self.settings = self.load_settings()
        
        # Initialize storage for last values
        self.last_counter_value = 0
        self.last_time_recorded = 0
        
        # ControlDesk variables
        self.controldesk = None
        self.controldesk_thread = None
        
        # --- Initialize tk Variables EARLIER --- 
        # Initialize all tk.Variables used in the GUI *before* creating widgets
        self.batpack_id = tk.StringVar(value=self.settings.get('batpack_id', "TestPack001"))
        self.cdp_path = tk.StringVar(value=self.settings.get('cdp_path', r"C:\00_BTF_Projects\01_Scalexio_Projects\03_BTF_DS_EMA_Prj\Instrumentation\CLD_Prj_EMA_IPB2_NCR24Q2_v24\CLD_Prj_EMA_IPB2_NCR24Q2_v24.CDP"))
        self.exp_name = tk.StringVar(value=self.settings.get('experiment_name', "BTF_Experiment"))
        self.cd_visible_var = tk.BooleanVar(value=self.settings.get("controldesk_visible", True))
        self.auto_record_var = tk.BooleanVar(value=self.settings.get("auto_record", False)) # Initialized early
        self.blf_pmz_csv_path_var = tk.StringVar(value=self.settings.get('blf_pmz_csv_path', ''))
        self.blf_debug_csv_path_var = tk.StringVar(value=self.settings.get('blf_debug_csv_path', ''))
        #====================================================
        self.blf_sdu_csv_path_var = tk.StringVar(value=self.settings.get('blf_sdu_csv_path', '')) # <-- ADDED FOR 3-CAN MODE
        self.can_mode_var = tk.StringVar(value=self.settings.get('can_mode', '2-CAN')) # <-- ADDED FOR 3-CAN MODE
        #====================================================

        # GCS upload settings variables
        self.test_name_var = tk.StringVar(value=self.settings.get('test_name', ''))
        self.ddg_environment_var = tk.StringVar(value=self.settings.get('ddg_environment', 'prod'))
        self.test_setup_var = tk.StringVar(value=self.settings.get('test_setup', ''))
        self.sample_no_var = tk.StringVar(value=self.settings.get('sample_no', ''))
        self.backend_api_url_var = tk.StringVar(value=self.settings.get('backend_api_url', '') or 'https://gemini-dash.jlr-apps.com/api')
        self.auto_upload_blf_var = tk.BooleanVar(value=self.settings.get('auto_upload_blf', True))
        self.blf_save_interval_var = tk.StringVar(value=str(self.settings.get('blf_save_interval', 60)))

        self.always_on_top = tk.BooleanVar(value=True) # Assuming default is True
        self.edit_paths_var = tk.BooleanVar(value=False)
        self.vbatt_state = tk.BooleanVar(value=False)
        self.blf_output_path_var = tk.StringVar(value="")

        # CAN Streaming variables
        self.stream_rig_id_var = tk.StringVar(value=self.settings.get('stream_rig_id', 'vib1_horizontal'))
        self.stream_ip_var = tk.StringVar(value=self.settings.get('stream_scalexio_ip', '192.168.0.10'))
        self.stream_fd_var = tk.BooleanVar(value=self.settings.get('stream_can_fd', False))
        self.stream_pubsub_project_var = tk.StringVar(value=self.settings.get('stream_pubsub_project', 'jlr-eng-ftd-tool-prod'))
        self.stream_pubsub_topic_var = tk.StringVar(value=self.settings.get('stream_pubsub_topic', 'sbtl-can-stream'))
        self.stream_manager = None
        self.stream_channel_vars = []  # List of (BooleanVar, channel_name) for checkboxes
        # --- End Early tk Variable Init ---
        
        # --- Create UI --- 
        self.create_menu()
        self.create_notebook_tabs() # Encapsulate tab creation
        self.create_log_area()      # Encapsulate log area creation
        
        # --- Finish Initialization ---
        self.apply_settings_to_ui() # Apply loaded settings to the now-created widgets
        self.log_message(f"Settings loaded from: {self.settings_file}")
        if self.settings.get('auto_record', False):
            self.log_message("Auto-start monitoring is enabled")

        self.update_timer_id = None
        self.updating_id = False
        self.load_logs()
        self.root.protocol("WM_DELETE_WINDOW", self.on_closing)
        if CONTROLDESK_AVAILABLE:
            self.root.after(1000, self.auto_launch_controldesk)
        else:
            self.log_message("ControlDesk module not available. Skipping auto-launch.")
            self.cd_status.config(text="Unavailable", foreground="gray")
            self.cd_start_button.config(state=tk.DISABLED)
            self.cd_close_button.config(state=tk.DISABLED)
            self.bus_record_button.config(state=tk.DISABLED)

        # --- Define Custom Style for Red Progress Bar ---
        self.style = ttk.Style(self.root)
        # Ensure the theme is set so user-defined styles work reliably
        current_theme = self.style.theme_use()
        self.style.theme_settings(current_theme, {
            "Red.Horizontal.TProgressbar": {
                "configure": {"background": "red", "troughcolor": "gray"},
                "map":       {"background": [("!disabled", "red")]}
            }
        })
        # --- End Style Definition ---

        # Re-grid disk space to correct row after realtime_frame layout
        self.disk_space_label.grid(row=4, column=0, padx=10, pady=5, sticky=tk.W)
        self.disk_space_progress.grid(row=4, column=1, padx=5, pady=5, sticky=tk.W+tk.E)
        self.disk_space_value_label.grid(row=4, column=2, padx=5, pady=5, sticky=tk.W)

        # In __init__, after creating all widgets and calling apply_settings_to_ui:
        # ...
        self.update_disk_space_display() # Initial call
        self.disk_space_timer_id = None # For periodic disk space updates
        # ...

    def create_notebook_tabs(self):
        """Creates the Notebook widget and its tabs."""
        self.notebook = ttk.Notebook(self.root)
        self.notebook.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S), padx=5, pady=5)

        # Create Test Operation Tab
        self.test_op_frame = ttk.Frame(self.notebook, padding="10")
        self.notebook.add(self.test_op_frame, text=' Test Operation ')
        self.test_op_frame.columnconfigure(0, weight=1)
        self._create_test_op_widgets() # Helper for widgets in this tab

        # Create BLF Converter Tab
        self.blf_converter_frame = ttk.Frame(self.notebook, padding="20")
        self.notebook.add(self.blf_converter_frame, text=' BLF Converter ')
        self.blf_converter_frame.columnconfigure(1, weight=1)
        self._create_blf_converter_widgets() # Helper for widgets in this tab

        # Disable converter tab if needed
        if not CONVERTER_AVAILABLE:
            try:
                converter_tab_index = self.notebook.index(self.blf_converter_frame)
                self.notebook.tab(converter_tab_index, state="disabled")
            except tk.TclError:
                print("Warning: Could not find BLF Converter tab to disable.")
            self.log_message("BLF Converter tab disabled: Conversion module not found.")

        # Create CAN Streaming Tab
        self.can_stream_frame = ttk.Frame(self.notebook, padding="10")
        self.notebook.add(self.can_stream_frame, text=' CAN Streaming ')
        self.can_stream_frame.columnconfigure(1, weight=1)
        self._create_can_stream_widgets()

        if not CAN_STREAMING_AVAILABLE:
            try:
                stream_tab_index = self.notebook.index(self.can_stream_frame)
                self.notebook.tab(stream_tab_index, state="disabled")
            except tk.TclError:
                pass
            self.log_message("CAN Streaming tab disabled: CanStreaming module not found.")
    

    def _create_test_op_widgets(self):
        """Creates widgets for the Test Operation tab."""
        # Add logo
        try:
            # Get the path to the logo file
            logo_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logo.jpg")
            
            # Check if the file exists
            if os.path.exists(logo_path):
                # Load and resize the image
                original_image = Image.open(logo_path)
                
                # Calculate the available width (full window width minus padding)
                # Use a default width initially, update on configure
                available_width = 860 # Default width (900 - 40px padding approx)
                
                # Calculate height with a reduced factor (0.6 times the original aspect ratio)
                width, height = original_image.size
                new_width = available_width
                new_height = int(height * (new_width / width) * 0.6)  # Reduced height by factor of 0.6
                resized_image = original_image.resize((new_width, new_height), Image.LANCZOS)
                
                # Convert to PhotoImage
                self.logo_image = ImageTk.PhotoImage(resized_image)
                
                # Create a label with background to display the image
                self.logo_frame = ttk.Frame(self.test_op_frame) # Parent changed
                self.logo_frame.grid(row=0, column=0, columnspan=3, sticky=(tk.W, tk.E), pady=5)  # Reduced padding
                self.test_op_frame.columnconfigure(0, weight=1) # Ensure frame column expands
                
                self.logo_label = ttk.Label(self.logo_frame, image=self.logo_image, anchor="center")
                self.logo_label.pack(fill=tk.X, expand=True)
                
                # Add window resize handler to update image size
                self.root.bind("<Configure>", self.on_window_resize)
                
                # Log success
                print("Logo loaded successfully")
            else:
                # Fallback to text title if logo file doesn't exist
                self.title_label = ttk.Label(self.test_op_frame, text="DSpace GUI", font=("Helvetica", 16)) # Parent changed
                self.title_label.grid(row=0, column=0, columnspan=3, pady=10)
                print(f"Logo file not found at: {logo_path}")
        except Exception as e:
            # Fallback to text title if there's an error
            self.title_label = ttk.Label(self.test_op_frame, text="DSpace GUI", font=("Helvetica", 16)) # Parent changed
            self.title_label.grid(row=0, column=0, columnspan=3, pady=10)
            print(f"Error loading logo: {str(e)}")
        
        # BatPack ID field (parent: self.test_op_frame)
        self.batpack_frame = ttk.Frame(self.test_op_frame) # Parent changed
        self.batpack_frame.grid(row=1, column=0, columnspan=3, pady=(5, 10), sticky=tk.W)
        
        self.batpack_label = ttk.Label(self.batpack_frame, text="BatPack ID:")
        self.batpack_label.grid(row=0, column=0, padx=5)
        
        # Get saved BatPack ID from settings or use default
        #saved_batpack_id = self.settings.get('batpack_id', "TestPack001")
        
       # self.batpack_id = tk.StringVar(value=saved_batpack_id)
        self.batpack_entry = ttk.Entry(self.batpack_frame, textvariable=self.batpack_id, width=30)
        self.batpack_entry.grid(row=0, column=1, padx=5)
   
        # Only bind to FocusOut and Return events, not the trace
        self.batpack_entry.bind("<FocusOut>", self.on_batpack_id_change)
        self.batpack_entry.bind("<Return>", self.on_batpack_id_change)
        
        # ControlDesk Project Path field (parent: self.test_op_frame)
        self.cdp_path_frame = ttk.Frame(self.test_op_frame) # Parent changed
        self.cdp_path_frame.grid(row=2, column=0, columnspan=3, pady=(5, 10), sticky=tk.W+tk.E)
        
        self.cdp_path_label = ttk.Label(self.cdp_path_frame, text="ControlDesk Project:")
        self.cdp_path_label.grid(row=0, column=0, padx=5, sticky=tk.W)
        
        # Get saved CDP path from settings or use default
        #saved_cdp_path = self.settings.get('cdp_path', r"C:\00_BTF_Projects\01_Scalexio_Projects\03_BTF_DS_EMA_Prj\Instrumentation\CLD_Prj_EMA_IPB2_NCR24Q2_v24\CLD_Prj_EMA_IPB2_NCR24Q2_v24.CDP")
        
        #self.cdp_path = tk.StringVar(value=saved_cdp_path)
        self.cdp_path_entry = ttk.Entry(self.cdp_path_frame, textvariable=self.cdp_path, width=60, state="readonly")
        self.cdp_path_entry.grid(row=0, column=1, padx=5, sticky=tk.W+tk.E)
        self.cdp_path_frame.columnconfigure(1, weight=1)
        
        # Add edit toggle button
        self.edit_paths_button = ttk.Button(
            self.cdp_path_frame,
            text="Edit",
            command=self.toggle_path_editing,
            width=6
        )
        self.edit_paths_button.grid(row=0, column=2, padx=5)
        
        self.cdp_browse_button = ttk.Button(self.cdp_path_frame, text="Browse...", command=self.browse_cdp_file)
        self.cdp_browse_button.grid(row=0, column=3, padx=5)
        
        # Add Experiment Name field
        self.exp_name_label = ttk.Label(self.cdp_path_frame, text="Experiment Name:")
        self.exp_name_label.grid(row=1, column=0, padx=5, pady=5, sticky=tk.W)
        
        # Get saved experiment name from settings or use default
        #saved_exp_name = self.settings.get('experiment_name', "BTF_Experiment")
        
        #self.exp_name = tk.StringVar(value=saved_exp_name)
        
        # Create a combobox that allows both selection from list and free input
        self.exp_name_entry = ttk.Combobox(self.cdp_path_frame, textvariable=self.exp_name, width=30, state="readonly")
        self.exp_name_entry.grid(row=1, column=1, padx=5, pady=5, sticky=tk.W)
        
        # Apply the disabled look directly to the widget using tkinter's configure method
        self.exp_name_entry.configure(background=self.readonly_bg)
        
        # Populate the dropdown list with experiment names
        self.update_experiment_list()
        
        # Button to refresh experiment list
        self.refresh_exp_button = ttk.Button(
            self.cdp_path_frame, 
            text="↻", 
            command=self.refresh_experiment_list,
            width=2
        )
        self.refresh_exp_button.grid(row=1, column=2, padx=5, pady=5)
        
        # Only bind to FocusOut and Return events
        self.cdp_path_entry.bind("<FocusOut>", self.on_cdp_path_change)
        self.cdp_path_entry.bind("<Return>", self.on_cdp_path_change)
        self.exp_name_entry.bind("<FocusOut>", self.on_exp_name_change)
        self.exp_name_entry.bind("<Return>", self.on_exp_name_change)
        
        # ControlDesk buttons frame (parent: self.test_op_frame)
        self.cd_frame = ttk.LabelFrame(self.test_op_frame, text="Control Desk") # Parent changed
        self.cd_frame.grid(row=3, column=0, columnspan=3, sticky=(tk.W, tk.E), pady=10, padx=5)
        
        self.cd_start_button = ttk.Button(self.cd_frame, text="Launch Control Desk", 
                                         command=self.launch_controldesk)
        self.cd_start_button.grid(row=0, column=0, padx=5, pady=5)
        
        self.cd_close_button = ttk.Button(self.cd_frame, text="Close Control Desk", 
                                          command=self.close_controldesk, state=tk.DISABLED)
        self.cd_close_button.grid(row=0, column=1, padx=5, pady=5)
        
        self.cd_status_label = ttk.Label(self.cd_frame, text="Status:")
        self.cd_status_label.grid(row=0, column=2, padx=5, pady=5)
        
        self.cd_status = ttk.Label(self.cd_frame, text="Not Running", foreground="red")
        self.cd_status.grid(row=0, column=3, padx=5, pady=5)
        
        # Initialize the visibility checkbox with saved setting
        saved_visibility = self.settings.get("controldesk_visible", True)
        self.cd_visible_checkbox = ttk.Checkbutton(
            self.cd_frame,
            text="Show ControlDesk Window",
            variable=self.cd_visible_var,
            command=self.toggle_controldesk_visibility
        )
        self.cd_visible_checkbox.grid(row=0, column=4, padx=5, pady=5)
        
        # Bus Record button frame (parent: self.test_op_frame)
        self.bus_record_frame = ttk.LabelFrame(self.test_op_frame, text="Bus Monitoring") # Parent changed
        self.bus_record_frame.grid(row=4, column=0, columnspan=3, sticky=(tk.W, tk.E), pady=10, padx=5)
        
        self.bus_record_button = ttk.Button(self.bus_record_frame, text="Start Bus Monitoring", 
                                           command=self.bus_record, state=tk.DISABLED)
        self.bus_record_button.grid(row=0, column=0, padx=5, pady=5)

        # Create the VBatt button with toggle functionality
        self.vbatt_button = ttk.Button(
            self.bus_record_frame,
            text="VBatt: OFF", # Initial text
            command=self.toggle_vbatt
        )
        self.vbatt_button.grid(row=0, column=2, padx=10, pady=5)

        # Create indicator label
        self.vbatt_status = ttk.Label(
            self.bus_record_frame,
            text="OFF", # Initial text
            foreground="red"
        )
        self.vbatt_status.grid(row=0, column=3, padx=10, pady=5)

        # <-- ADDED FOR 3-CAN MODE: CAN Mode Selector -->
        self.can_mode_label = ttk.Label(self.bus_record_frame, text="CAN Mode:")
        self.can_mode_label.grid(row=0, column=4, padx=(20, 5), pady=5)
        
        self.can_mode_combo = ttk.Combobox(self.bus_record_frame, textvariable=self.can_mode_var,
                                           values=["2-CAN", "3-CAN"], state="readonly", width=8)
        self.can_mode_combo.grid(row=0, column=5, padx=5, pady=5)
        self.can_mode_combo.bind("<<ComboboxSelected>>", self.on_can_mode_change)

        # BLF Save Interval (next to CAN Mode)
        self.blf_interval_label = ttk.Label(self.bus_record_frame, text="Save BLF Every:")
        self.blf_interval_label.grid(row=0, column=6, padx=(20, 5), pady=5)

        self.blf_interval_combo = ttk.Combobox(
            self.bus_record_frame, textvariable=self.blf_save_interval_var,
            values=["3", "30", "60", "90", "120", "240"], state="readonly", width=5
        )
        self.blf_interval_combo.grid(row=0, column=7, padx=5, pady=5)

        self.blf_interval_unit_label = ttk.Label(self.bus_record_frame, text="min")
        self.blf_interval_unit_label.grid(row=0, column=8, padx=(0, 5), pady=5)

        # GCS Upload Settings LabelFrame (parent: self.test_op_frame)
        self.gcs_frame = ttk.LabelFrame(self.test_op_frame, text="GCS Upload Settings")
        self.gcs_frame.grid(row=4, column=0, columnspan=3, sticky=(tk.W, tk.E), pady=5, padx=5)
        # Shift bus_record_frame, realtime_frame row indices down by inserting GCS frame
        self.bus_record_frame.grid(row=5, column=0, columnspan=3, sticky=(tk.W, tk.E), pady=10, padx=5)

        # SBTL Test Number
        ttk.Label(self.gcs_frame, text="SBTL Test:").grid(row=0, column=0, padx=5, pady=3, sticky=tk.W)
        self.sbtl_test_combo = ttk.Combobox(self.gcs_frame, textvariable=self.test_name_var, width=20)
        self.sbtl_test_combo.grid(row=0, column=1, padx=5, pady=3, sticky=tk.W)
        ttk.Button(self.gcs_frame, text="↻", command=self.fetch_sbtl_tests, width=3).grid(row=0, column=2, padx=5, pady=3)

        # Auto-upload checkbox
        ttk.Checkbutton(self.gcs_frame, text="Auto-upload BLF to GCS", variable=self.auto_upload_blf_var).grid(row=0, column=3, padx=15, pady=3, sticky=tk.W)

        # Real-time values frame (parent: self.test_op_frame)
        self.realtime_frame = ttk.LabelFrame(self.test_op_frame, text="Real-Time Values") # Parent changed
        self.realtime_frame.grid(row=6, column=0, columnspan=3, sticky=(tk.W, tk.E), pady=10, padx=5)
        # LV voltage display
        self.lv_voltage_label = ttk.Label(self.realtime_frame, text="LV Voltage:")
        self.lv_voltage_label.grid(row=3, column=0, padx=10, pady=5, sticky=tk.W)

        self.lv_voltage_value = ttk.Label(self.realtime_frame, text="0.00 V", width=10)
        self.lv_voltage_value.grid(row=3, column=1, padx=10, pady=5, sticky=tk.W)

        # LV current display
        self.lv_current_label = ttk.Label(self.realtime_frame, text="LV Current:")
        self.lv_current_label.grid(row=3, column=2, padx=10, pady=5, sticky=tk.W)

        self.lv_current_value = ttk.Label(self.realtime_frame, text="0.00 A", width=10)
        self.lv_current_value.grid(row=3, column=3, padx=10, pady=5, sticky=tk.W)
        
        # Recorder counter display — prominent, own row, 3x size
        self.recorder_counter_label = ttk.Label(self.realtime_frame, text="Recorder Counter:",
                                                font=("Helvetica", 14, "bold"))
        self.recorder_counter_label.grid(row=0, column=0, padx=10, pady=8, sticky=tk.W)

        self.recorder_counter_value = ttk.Label(self.realtime_frame, text="0",
                                                font=("Helvetica", 24, "bold"), foreground="green", width=12)
        self.recorder_counter_value.grid(row=0, column=1, columnspan=3, padx=10, pady=8, sticky=tk.W)

        # Recording status — own row below counter
        self.recording_status_label = ttk.Label(self.realtime_frame, text="Recording Status:",
                                                font=("Helvetica", 11))
        self.recording_status_label.grid(row=1, column=0, padx=10, pady=5, sticky=tk.W)

        self.recording_status_value = ttk.Label(self.realtime_frame, text="Idle",
                                                font=("Helvetica", 11, "bold"), foreground="blue")
        self.recording_status_value.grid(row=1, column=1, padx=10, pady=5, sticky=tk.W)

        # Time recorded display
        self.time_recorded_label = ttk.Label(self.realtime_frame, text="Time Recorded:")
        self.time_recorded_label.grid(row=2, column=0, padx=10, pady=5, sticky=tk.W)

        self.time_recorded_value = ttk.Label(self.realtime_frame, text="00:00:00", width=10)
        self.time_recorded_value.grid(row=2, column=1, padx=10, pady=5, sticky=tk.W)
        
        # Disk Space Display (within self.realtime_frame)
        self.disk_space_label = ttk.Label(self.realtime_frame, text="C: Drive Space:")
        self.disk_space_label.grid(row=4, column=0, padx=10, pady=5, sticky=tk.W)

        self.disk_space_progress = ttk.Progressbar(self.realtime_frame, orient="horizontal", length=150, mode="determinate")
        self.disk_space_progress.grid(row=4, column=1, padx=5, pady=5, sticky=tk.W+tk.E)

        self.disk_space_value_label = ttk.Label(self.realtime_frame, text="N/A", width=15)
        self.disk_space_value_label.grid(row=4, column=2, padx=5, pady=5, sticky=tk.W)

    def _create_blf_converter_widgets(self):
        """Creates widgets for the BLF Converter tab."""
        # --- BLF Converter UI Elements ---
        
        # PMZ CSV File Input
        ttk.Label(self.blf_converter_frame, text="PMZ CAN CSV File:").grid(row=0, column=0, padx=5, pady=10, sticky=tk.W)
        self.blf_pmz_entry = ttk.Entry(self.blf_converter_frame, textvariable=self.blf_pmz_csv_path_var, width=70)
        self.blf_pmz_entry.grid(row=0, column=1, padx=5, pady=10, sticky=(tk.W, tk.E))
        self.blf_pmz_browse_btn = ttk.Button(self.blf_converter_frame, text="Browse...", command=lambda: self.browse_csv_file(self.blf_pmz_csv_path_var, "PMZ"))
        self.blf_pmz_browse_btn.grid(row=0, column=2, padx=5, pady=10)
        
        # Debug CSV File Input
        ttk.Label(self.blf_converter_frame, text="Debug CAN CSV File:").grid(row=1, column=0, padx=5, pady=10, sticky=tk.W)
        self.blf_debug_entry = ttk.Entry(self.blf_converter_frame, textvariable=self.blf_debug_csv_path_var, width=70)
        self.blf_debug_entry.grid(row=1, column=1, padx=5, pady=10, sticky=(tk.W, tk.E))
        self.blf_debug_browse_btn = ttk.Button(self.blf_converter_frame, text="Browse...", command=lambda: self.browse_csv_file(self.blf_debug_csv_path_var, "Debug"))
        self.blf_debug_browse_btn.grid(row=1, column=2, padx=5, pady=10)

        # <-- ADDED FOR 3-CAN MODE: SDU CSV File Input -->
        ttk.Label(self.blf_converter_frame, text="SDU CAN CSV File:").grid(row=2, column=0, padx=5, pady=10, sticky=tk.W)
        self.blf_sdu_entry = ttk.Entry(self.blf_converter_frame, textvariable=self.blf_sdu_csv_path_var, width=70)
        self.blf_sdu_entry.grid(row=2, column=1, padx=5, pady=10, sticky=(tk.W, tk.E))
        self.blf_sdu_browse_btn = ttk.Button(self.blf_converter_frame, text="Browse...", command=lambda: self.browse_csv_file(self.blf_sdu_csv_path_var, "SDU"))
        self.blf_sdu_browse_btn.grid(row=2, column=2, padx=5, pady=10)
        
        # Output BLF File Path Label (Optional Display)
        self.blf_output_path_label_info = ttk.Label(self.blf_converter_frame, text="Output BLF Path:")
        self.blf_output_path_label_info.grid(row=2, column=0, padx=5, pady=10, sticky=tk.W)
        self.blf_output_path_var = tk.StringVar(value="") # To display generated path
        self.blf_output_path_label = ttk.Label(self.blf_converter_frame, textvariable=self.blf_output_path_var, relief="sunken", width=70, anchor=tk.W)
        self.blf_output_path_label.grid(row=2, column=1, columnspan=2, padx=5, pady=10, sticky=(tk.W, tk.E))
        # Bind path changes to update output path display
        # self.blf_pmz_csv_path_var.trace_add("write", self.update_output_blf_path_display) # REMOVED TRACE
        self.blf_pmz_entry.bind("<FocusOut>", self.update_output_blf_path_display)
        self.blf_pmz_entry.bind("<Return>", self.update_output_blf_path_display)
        self.blf_debug_entry.bind("<FocusOut>", self.update_output_blf_path_display)
        self.blf_debug_entry.bind("<Return>", self.update_output_blf_path_display)
        self.blf_sdu_entry.bind("<FocusOut>", self.update_output_blf_path_display)
        self.blf_sdu_entry.bind("<Return>", self.update_output_blf_path_display)
        
        # Start Conversion Button
        self.blf_start_btn = ttk.Button(self.blf_converter_frame, text="Start Conversion", command=self.start_blf_conversion)
        self.blf_start_btn.grid(row=3, column=0, columnspan=3, pady=20)
        
    # ================================================================== #
    # CAN Streaming Tab
    # ================================================================== #

    def _create_can_stream_widgets(self):
        """Creates widgets for the CAN Streaming tab."""
        row = 0

        # --- Rig Configuration ---
        config_frame = ttk.LabelFrame(self.can_stream_frame, text="Rig Configuration", padding="10")
        config_frame.grid(row=row, column=0, columnspan=3, sticky=(tk.W, tk.E), pady=5)
        config_frame.columnconfigure(1, weight=1)

        ttk.Label(config_frame, text="Rig ID:").grid(row=0, column=0, padx=5, pady=3, sticky=tk.W)
        self.stream_rig_combo = ttk.Combobox(config_frame, textvariable=self.stream_rig_id_var,
                                             values=["vib1_horizontal", "vib2_vertical", "mast"], width=25)
        self.stream_rig_combo.grid(row=0, column=1, padx=5, pady=3, sticky=tk.W)

        ttk.Label(config_frame, text="SCALEXIO IP:").grid(row=1, column=0, padx=5, pady=3, sticky=tk.W)
        self.stream_ip_entry = ttk.Entry(config_frame, textvariable=self.stream_ip_var, width=20)
        self.stream_ip_entry.grid(row=1, column=1, padx=5, pady=3, sticky=tk.W)

        self.stream_fd_check = ttk.Checkbutton(config_frame, text="Enable CAN FD", variable=self.stream_fd_var)
        self.stream_fd_check.grid(row=2, column=0, columnspan=2, padx=5, pady=3, sticky=tk.W)

        row += 1

        # --- Channel Selection ---
        channel_frame = ttk.LabelFrame(self.can_stream_frame, text="CAN Channels", padding="10")
        channel_frame.grid(row=row, column=0, columnspan=3, sticky=(tk.W, tk.E), pady=5)
        channel_frame.columnconfigure(0, weight=1)

        btn_frame = ttk.Frame(channel_frame)
        btn_frame.grid(row=0, column=0, sticky=tk.W)
        self.stream_discover_btn = ttk.Button(btn_frame, text="Discover Channels", command=self._stream_discover_channels)
        self.stream_discover_btn.pack(side=tk.LEFT, padx=5)

        self.stream_select_all_btn = ttk.Button(btn_frame, text="Select All", command=self._stream_select_all)
        self.stream_select_all_btn.pack(side=tk.LEFT, padx=5)

        self.stream_channel_list_frame = ttk.Frame(channel_frame)
        self.stream_channel_list_frame.grid(row=1, column=0, sticky=(tk.W, tk.E), pady=5)

        self.stream_channel_status = ttk.Label(channel_frame, text="No channels discovered yet.", foreground="gray")
        self.stream_channel_status.grid(row=2, column=0, sticky=tk.W, padx=5)

        row += 1

        # --- Streaming Control ---
        control_frame = ttk.LabelFrame(self.can_stream_frame, text="Streaming Control", padding="10")
        control_frame.grid(row=row, column=0, columnspan=3, sticky=(tk.W, tk.E), pady=5)
        control_frame.columnconfigure(1, weight=1)

        self.stream_toggle_btn = ttk.Button(control_frame, text="Enable Streaming", command=self._stream_toggle)
        self.stream_toggle_btn.grid(row=0, column=0, padx=5, pady=5)

        self.stream_status_label = ttk.Label(control_frame, text="Stopped", foreground="gray", font=("Helvetica", 10, "bold"))
        self.stream_status_label.grid(row=0, column=1, padx=10, pady=5, sticky=tk.W)

        # Live stats
        stats_frame = ttk.Frame(control_frame)
        stats_frame.grid(row=1, column=0, columnspan=3, sticky=(tk.W, tk.E), pady=5)

        ttk.Label(stats_frame, text="Rate:").pack(side=tk.LEFT, padx=5)
        self.stream_rate_label = ttk.Label(stats_frame, text="0 msg/s", width=12)
        self.stream_rate_label.pack(side=tk.LEFT, padx=5)

        # Overrun warning (hidden unless overruns occur)
        self.stream_overrun_label = ttk.Label(stats_frame, text="", foreground="red")
        self.stream_overrun_label.pack(side=tk.LEFT, padx=10)

        # Recording override + indicator
        rec_frame = ttk.Frame(control_frame)
        rec_frame.grid(row=2, column=0, columnspan=3, sticky=tk.W, pady=3)

        ttk.Label(rec_frame, text="Cloud Saving:").pack(side=tk.LEFT, padx=5)
        self.stream_record_mode_var = tk.StringVar(value="Auto (12V)")
        self.stream_record_mode_combo = ttk.Combobox(
            rec_frame, textvariable=self.stream_record_mode_var,
            values=["Auto (12V)", "Always Save to Cloud", "Disabled"],
            state="readonly", width=14)
        self.stream_record_mode_combo.pack(side=tk.LEFT, padx=5)
        self.stream_record_mode_combo.bind("<<ComboboxSelected>>", self._stream_record_mode_changed)
        self.stream_recording_label = ttk.Label(rec_frame, text="OFF", foreground="gray",
                                                font=("Helvetica", 10, "bold"))
        self.stream_recording_label.pack(side=tk.LEFT, padx=5)

    def _stream_discover_channels(self):
        """Discover CAN channels on the SCALEXIO and populate checkboxes."""
        if not CAN_STREAMING_AVAILABLE:
            self.log_message("[CAN Stream] Module not available.")
            return

        ip = self.stream_ip_var.get().strip()
        rig_id = self.stream_rig_id_var.get().strip()

        self.stream_channel_status.config(text="Discovering...", foreground="blue")
        self.root.update_idletasks()

        self.stream_manager = CanStreamManager(rig_id=rig_id, ip_address=ip, log_callback=self.safe_log_message)
        channels = self.stream_manager.discover_channels()

        # Clear old checkboxes
        for widget in self.stream_channel_list_frame.winfo_children():
            widget.destroy()
        self.stream_channel_vars = []

        if not channels:
            self.stream_channel_status.config(text="No channels found. Check IP and hardware.", foreground="red")
            return

        names = self.stream_manager.get_channel_names()
        for i, name in enumerate(names):
            var = tk.BooleanVar(value=True)  # Default: all selected
            cb = ttk.Checkbutton(self.stream_channel_list_frame, text=f"[{i}] {name}", variable=var)
            cb.grid(row=i, column=0, sticky=tk.W, padx=10)
            self.stream_channel_vars.append((var, name))

        self.stream_channel_status.config(text=f"Found {len(channels)} channel(s). All selected.", foreground="green")
        self.log_message(f"[CAN Stream] Discovered {len(channels)} channels at {ip}")

    def _stream_select_all(self):
        """Select all discovered channels."""
        for var, _ in self.stream_channel_vars:
            var.set(True)

    def _stream_toggle(self):
        """Toggle streaming on/off."""
        if not CAN_STREAMING_AVAILABLE:
            messagebox.showerror("Error", "CAN Streaming module not available.")
            return

        if self.stream_manager and self.stream_manager.is_streaming:
            self._stream_stop()
        else:
            self._stream_start()

    def _stream_start(self):
        """Start CAN streaming."""
        if not self.stream_manager:
            # Auto-discover if not done yet
            self._stream_discover_channels()
            if not self.stream_manager or not self.stream_manager._channels:
                messagebox.showwarning("No Channels", "Could not discover CAN channels. Check IP address.")
                return

        # Set selected channels
        selected = [i for i, (var, _) in enumerate(self.stream_channel_vars) if var.get()]
        if not selected:
            messagebox.showwarning("No Channels", "Please select at least one CAN channel.")
            return
        self.stream_manager.set_selected_channels(selected)

        # Set rig ID and batpack
        self.stream_manager.rig_id = self.stream_rig_id_var.get().strip()
        self.stream_manager.batpack_id = self.batpack_id.get()

        # Setup Pub/Sub (uses settings, not shown in UI)
        project = self.settings.get('stream_pubsub_project', 'jlr-eng-ftd-tool-prod')
        topic = self.settings.get('stream_pubsub_topic', 'sbtl-can-stream')
        self.stream_manager.setup_pubsub(project, topic)

        # Save settings
        self._save_stream_settings()

        # Start
        self.stream_manager.start_streaming(
            fd=self.stream_fd_var.get(),
            stats_callback=self._stream_stats_update,
        )

        self.stream_toggle_btn.config(text="Disable Streaming")
        self.stream_status_label.config(text="STREAMING", foreground="green")
        self.stream_discover_btn.config(state=tk.DISABLED)
        self.log_message(f"[CAN Stream] Started streaming ({len(selected)} channels, rig={self.stream_manager.rig_id})")

    def _stream_stop(self):
        """Stop CAN streaming."""
        if self.stream_manager:
            self.stream_manager.stop_streaming()

        self.stream_toggle_btn.config(text="Enable Streaming")
        self.stream_status_label.config(text="Stopped", foreground="gray")
        self.stream_discover_btn.config(state=tk.NORMAL)

    def _stream_stats_update(self, msg_per_sec, total, overruns):
        """Called from streaming thread — schedule GUI update on main thread."""
        try:
            self.root.after(0, self._stream_update_labels, msg_per_sec, total, overruns)
        except Exception:
            pass

    def _stream_update_labels(self, msg_per_sec, total, overruns):
        """Update stats labels on the GUI thread."""
        self.stream_rate_label.config(text=f"{msg_per_sec} msg/s")

        # Only show overrun warning if messages were lost
        if overruns > 0:
            self.stream_overrun_label.config(text=f"WARNING: {overruns} message(s) lost!")
        else:
            self.stream_overrun_label.config(text="")

        # Update recording state based on mode
        if self.stream_manager:
            mode = self.stream_record_mode_var.get()
            if mode == "Always Save to Cloud":
                is_recording = True
            elif mode == "Disabled":
                is_recording = False
            else:  # Auto (12V)
                is_recording = self.vbatt_state.get()
            self.stream_manager.set_recording(is_recording)
            self._update_cloud_saving_label(is_recording, mode)

    def _update_cloud_saving_label(self, is_recording, mode):
        """Update the cloud saving status label."""
        if is_recording:
            if mode == "Always Save to Cloud":
                self.stream_recording_label.config(text="ON (always)", foreground="green")
            else:
                self.stream_recording_label.config(text="ON (12V)", foreground="green")
        else:
            if mode == "Disabled":
                self.stream_recording_label.config(text="OFF (disabled)", foreground="red")
            else:
                self.stream_recording_label.config(text="OFF", foreground="gray")

    def _stream_record_mode_changed(self, event=None):
        """Handle cloud saving mode change."""
        mode = self.stream_record_mode_var.get()
        if self.stream_manager:
            if mode == "Always Save to Cloud":
                self.stream_manager.set_recording(True)
                self.log_message("[CAN Stream] Cloud saving: always save to cloud (12V ignored)")
            elif mode == "Disabled":
                self.stream_manager.set_recording(False)
                self.log_message("[CAN Stream] Cloud saving: disabled (12V ignored)")
            else:
                is_12v = self.vbatt_state.get()
                self.stream_manager.set_recording(is_12v)
                self.log_message("[CAN Stream] Cloud saving: auto (following 12V state)")
            self._update_cloud_saving_label(self.stream_manager._recording, mode)

    def _save_stream_settings(self):
        """Persist CAN streaming settings."""
        self.settings['stream_rig_id'] = self.stream_rig_id_var.get()
        self.settings['stream_scalexio_ip'] = self.stream_ip_var.get()
        self.settings['stream_can_fd'] = self.stream_fd_var.get()
        self.settings['stream_pubsub_project'] = self.stream_pubsub_project_var.get()
        self.settings['stream_pubsub_topic'] = self.stream_pubsub_topic_var.get()
        self.save_settings()

    def _update_vbatt_button_state(self, is_on):
        """Helper function to update the VBatt button and label UI elements."""
        self.vbatt_state.set(is_on)
        if is_on:
            self.vbatt_button.config(text="VBatt: ON")
            self.vbatt_status.config(text="ON", foreground="green")
        else:
            self.vbatt_button.config(text="VBatt: OFF")
            self.vbatt_status.config(text="OFF", foreground="red")
            
    def _check_and_update_initial_vbatt_state(self):
        """Checks the initial LV voltage from ControlDesk and updates the UI."""
        if self.controldesk and hasattr(self.controldesk, 'get_lv_voltage'):
            initial_voltage = self.controldesk.get_lv_voltage()
            if initial_voltage is not None:
                is_initially_on = initial_voltage > 10.0
                self.log_message(f"Initial LV Voltage check: {initial_voltage:.2f}V. VBatt state: {'ON' if is_initially_on else 'OFF'}")
                # Update UI in the main thread
                self.root.after(0, lambda: self._update_vbatt_button_state(is_initially_on))
            else:
                self.log_message("Could not read initial LV voltage. Assuming OFF.")
                # Ensure UI reflects OFF state if reading failed
                self.root.after(0, lambda: self._update_vbatt_button_state(False))

    def toggle_vbatt(self):
        """Toggle the VBatt state between ON and OFF"""
        if not self.controldesk:
            messagebox.showwarning("Warning", "Control Desk is not running")
            return
        
        # Toggle the state based on the *current* variable state
        current_state = self.vbatt_state.get()
        new_state = not current_state
        
        # If trying to turn VBatt ON, check disk space
        if new_state: # If True (meaning trying to turn ON)
            free_space_gb = self.get_c_drive_free_space_gb()
            if free_space_gb < 6:
                if not messagebox.askyesno("Low Disk Space Warning", 
                                          f"C: drive has less than {free_space_gb:.1f} GB free (less than 6 GB recommended).\nAre you sure you want to turn ON VBatt? This might affect recording."):
                    self.log_message("VBatt ON operation cancelled due to low disk space warning.")
                    return # User cancelled

        # Log the action
        action = "ON" if new_state else "OFF"
        self.log_message(f"Setting VBatt to {action}")
        
        # Send the value to ControlDesk
        try:
            # Convert boolean to int (1 for ON, 0 for OFF)
            vbatt_value = 1 if new_state else 0
            
            # Send to ControlDesk
            success = False
            if hasattr(self.controldesk, 'set_vbatt_state'):
                success = self.controldesk.set_vbatt_state(vbatt_value)
            else:
                self.log_message("Warning: ControlDesk doesn't have set_vbatt_state method")
                
            # Only update the UI if the command was sent successfully
            if success:
                self._update_vbatt_button_state(new_state)
            else:
                # Revert the attempted change if sending failed
                self.log_message(f"Failed to set VBatt state in ControlDesk. Reverting UI.")
                messagebox.showerror("Error", f"Failed to set VBatt state in ControlDesk.")
                # No need to revert self.vbatt_state as it wasn't updated yet
                # We only update the button based on the current self.vbatt_state
                self._update_vbatt_button_state(current_state)
                
        except Exception as e:
            self.log_message(f"Error setting VBatt state: {str(e)}")
            messagebox.showerror("Error", f"Failed to set VBatt state:\n{str(e)}")
            # Revert UI on other exceptions
            self._update_vbatt_button_state(current_state)

    def toggle_controldesk_visibility(self):
        """Toggle the visibility of the ControlDesk main window"""
        if not self.controldesk:
            # ControlDesk not running, just log and return
            self.log_message("ControlDesk is not running - visibility setting will apply when launched")
            # Save the setting anyway
            visible = self.cd_visible_var.get()
            self.settings['controldesk_visible'] = visible
            self.save_settings()
            return
        
        try:
            # Get the desired visibility state
            visible = self.cd_visible_var.get()
            
            # Save the visibility setting to settings
            self.settings['controldesk_visible'] = visible
            self.save_settings()
            
            # Apply the visibility setting
            if hasattr(self.controldesk, 'ControlDeskApplication') and self.controldesk.ControlDeskApplication:
                if hasattr(self.controldesk.ControlDeskApplication, 'MainWindow'):
                    self.controldesk.ControlDeskApplication.MainWindow.Visible = visible
                    self.log_message(f"ControlDesk window visibility set to: {'Visible' if visible else 'Hidden'}")
                else:
                    self.log_message("Could not access ControlDesk MainWindow property")
            else:
                self.log_message("Could not access ControlDesk Application")
        except Exception as e:
            self.log_message(f"Error toggling ControlDesk visibility: {str(e)}")

    def auto_launch_controldesk(self):
        """Automatically launch ControlDesk on startup"""
        if not self.controldesk:
            # Get paths from settings
            cdp_path = self.cdp_path.get()
            
            # Check if the CDP file exists
            if not os.path.exists(cdp_path):
                self.log_message(f"Auto-launch skipped: ControlDesk project file not found at: {cdp_path}")
                return
                
            # Use visibility setting from checkbox/settings
            visible = self.cd_visible_var.get()
            mode_text = "visible" if visible else "invisible"
            self.log_message(f"Auto-launching Control Desk in {mode_text} mode...")
            
            # Start ControlDesk in a separate thread
            self.controldesk_thread = threading.Thread(
                target=self._auto_start_controldesk_worker, # Use a renamed worker
                args=(cdp_path, self.exp_name.get(), visible), # Pass visibility
                daemon=True
            )
            self.controldesk_thread.start()
        else:
            self.log_message("Control Desk is already running")
            
    def _auto_start_controldesk_worker(self, cdp_path, experiment_name, visible):
        """Worker thread to auto-start ControlDesk"""
        import pythoncom
        pythoncom.CoInitialize()
        try:
            # Change status indicator
            self.root.after(0, lambda: self.cd_status.config(text="Starting...", foreground="orange"))
            
            # Import here to avoid circular import
            from ControlDesk import dSpace_ControlDesk
            
            # Create ControlDesk instance with callback to our log function
            self.controldesk = dSpace_ControlDesk(log_callback=self.log_message)
            
            # Launch ControlDesk with the selected project and experiment
            success = self.controldesk.startApp(cdp_path, experiment_name, visible=visible)
            
            if success:
                # Update the status in the GUI thread
                self.root.after(0, lambda: self.cd_status.config(text="Running", foreground="green"))
                self.root.after(0, lambda: self.bus_record_button.config(state=tk.NORMAL))
                self.root.after(0, lambda: self.cd_close_button.config(state=tk.NORMAL)) # Enable close button
                
                # Set the BatPack ID
                batpack_id = self.batpack_id.get()
                if batpack_id:
                    self.controldesk.set_batpack_id(batpack_id)
                
                # <-- ADDED FOR 3-CAN MODE -->
                self.controldesk.set_can_mode(self.can_mode_var.get())

                # Pass GCS upload settings to ControlDesk
                self.controldesk.auto_upload_blf = self.auto_upload_blf_var.get()
                self.controldesk.test_name = self.test_name_var.get()
                self.controldesk.test_setup = self.test_setup_var.get()
                self.controldesk.sample_no = self.sample_no_var.get()
                self.controldesk.ddg_environment = self.ddg_environment_var.get()
                self.controldesk.backend_api_url = self.backend_api_url_var.get()
                self.controldesk.api_token = self.settings.get('api_token', '')

                # *** Check and update initial VBatt state ***
                self._check_and_update_initial_vbatt_state()
                
                # Start the periodic updates
                self.root.after(1000, self.start_periodic_updates)
                
                # Check if auto-record is enabled - try multiple approaches
                auto_record = False
            
                try:
                    # Primary source: the variable value
                    if hasattr(self, 'auto_record_var'):
                        auto_record = bool(self.auto_record_var.get())
                        self.log_message(f"Auto-record enabled: {auto_record}")
                except Exception as e:
                    self.log_message(f"Error getting auto-record variable: {str(e)}")
                    # Fallbacks only if the primary approach fails
                    try:
                        # Fallback 1: Check the checkbox state directly
                        if hasattr(self, 'auto_record_checkbox') and isinstance(self.auto_record_checkbox, ttk.Checkbutton):
                            checkbox_selected = 'selected' in self.auto_record_checkbox.state()
                            self.log_message(f"Auto-record from checkbox state: {checkbox_selected}")
                            if checkbox_selected:
                                auto_record = True
                    except Exception as e2:
                        self.log_message(f"Error checking checkbox state: {str(e2)}")
                        
                        # Fallback 2: Check settings directly
                        try:
                            settings_auto_record = bool(self.settings.get('auto_record', False))
                            self.log_message(f"Auto-record from settings: {settings_auto_record}")
                            auto_record = settings_auto_record
                        except Exception as e3:
                            self.log_message(f"Error loading auto-record from settings: {str(e3)}")
                
                if auto_record:
                    self.log_message("Auto-start recording enabled. Starting in 5 seconds...")
                    
                    # Schedule bus recording to start after 5 seconds
                    self.root.after(5000, self._auto_start_recording)
            else:
                # Update the status in the GUI thread
                self.root.after(0, lambda: self.cd_status.config(text="Failed to start", foreground="red"))
                self.root.after(0, lambda: self.cd_start_button.config(state=tk.NORMAL)) # Re-enable start

        except Exception as e:
            import traceback
            error_message = f"Error auto-launching Control Desk: {str(e)}\n{traceback.format_exc()}"
            self.log_message(error_message)
            
            # Update the status in the GUI thread
            self.root.after(0, lambda: self.cd_status.config(text="Error", foreground="red"))
            self.root.after(0, lambda: self.cd_start_button.config(state=tk.NORMAL)) # Re-enable start

    def create_menu(self):
        """Create the top menu bar"""
        menubar = tk.Menu(self.root)
        self.root.config(menu=menubar)
        
        # File menu
        file_menu = tk.Menu(menubar, tearoff=0)
        menubar.add_cascade(label="File", menu=file_menu)
        file_menu.add_command(label="Open Log Directory", command=self.open_log_directory)
        file_menu.add_command(label="Save Settings", command=self.save_settings)
        file_menu.add_command(label="Reload Settings", command=self.reload_settings)
        file_menu.add_command(label="Repair Settings File", command=self.repair_settings_file)
        file_menu.add_separator()
        file_menu.add_command(label="Clear Log File", command=self.clear_log)
        file_menu.add_separator()
        file_menu.add_command(label="Exit", command=self.on_closing) # Use on_closing for exit

            # Add Always on Top toggle
        self.always_on_top = tk.BooleanVar(value=True)
        file_menu.add_checkbutton(label="Always on Top", variable=self.always_on_top, 
                                command=self.toggle_always_on_top)
        
        # Control menu 
        control_menu = tk.Menu(menubar, tearoff=0)
        menubar.add_cascade(label="Control", menu=control_menu)
        # control_menu.add_command(label="Start", command=self.start_action) # Removed generic start
        # control_menu.add_command(label="Stop", command=self.stop_action) # Removed generic stop
        # control_menu.add_separator()
        control_menu.add_command(label="Launch Control Desk", command=self.launch_controldesk)
        control_menu.add_command(label="Close Control Desk", command=self.close_controldesk)
        control_menu.add_command(label="Start Bus Monitoring", command=self.bus_record)
        control_menu.add_separator()
        control_menu.add_command(label="Toggle VBatt", command=self.toggle_vbatt)
        
        # Help menu
        help_menu = tk.Menu(menubar, tearoff=0)
        menubar.add_cascade(label="Help", menu=help_menu)
        help_menu.add_command(label="About", command=self.show_about)
        help_menu.add_command(label="Help", command=self.show_help)

    def toggle_always_on_top(self):
        """Toggle whether the window stays on top of other windows"""
        is_top = self.always_on_top.get()
        self.root.attributes('-topmost', is_top)
        self.log_message(f"Always on top: {'Enabled' if is_top else 'Disabled'}")

    def open_log_directory(self):
        """Open the directory containing log files"""
        log_dir = os.path.dirname(os.path.abspath(self.log_file))
        try:
            # Open the directory in file explorer
            if os.name == 'nt':  # Windows
                os.startfile(log_dir)
            elif os.name == 'posix':  # macOS and Linux
                try:
                    import subprocess
                    subprocess.Popen(['open', log_dir]) # macOS
                except FileNotFoundError:
                     subprocess.Popen(['xdg-open', log_dir]) # Linux
            else:
                 self.log_message("Unsupported OS for opening directory.")

        except Exception as e:
            self.log_message(f"Error opening log directory: {str(e)}")
            messagebox.showerror("Error", f"Could not open log directory:\n{str(e)}")

    def show_about(self):
        """Show about dialog"""
        messagebox.showinfo("About", "dSpace GUI Application\n\nVersion 1.1\n\nDeveloped for BatLab")

    def show_help(self):
        """Show help dialog"""
        help_text = """
        dSpace GUI Application Help
        
        Test Operation Tab:
        - BatPack ID: Enter the ID for the current battery pack.
        - ControlDesk Project/Experiment: Select the dSpace project and experiment. Use 'Edit' to change paths.
        - Launch/Close Control Desk: Manage the dSpace ControlDesk application.
        - Start Bus Monitoring: Begin recording CAN bus data (requires ControlDesk).
        - Auto-start Monitoring: Automatically start monitoring when ControlDesk launches.
        - VBatt: Toggle the simulated battery voltage output.
        - Real-Time Values: Displays current readings from dSpace.

        BLF Converter Tab:
        - Select PMZ and Debug CSV files recorded by dSpace.
        - The output BLF file path will be generated automatically.
        - Click 'Start Conversion' to merge the CSVs into a BLF file.

        File Menu:
        - Open Log Directory: View log files.
        - Save/Reload Settings: Manage application settings.
        - Repair Settings: Recreate settings file if corrupted.
        - Clear Log File: Erase the content of the current log file.
        - Always on Top: Keep the GUI window above others.
        - Exit: Close the application safely.

        For more help, consult the documentation or contact support.
        """
        messagebox.showinfo("Help", help_text)

    def safe_log_message(self, message):
        """Safe logging method for use during initialization"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        full_message = f"[{timestamp}] {message}\n"
        print(full_message.strip()) # Print to console
        
        # Save to log file even if UI isn't ready
        try:
            with open(self.log_file, "a", encoding='utf-8') as log_file:
                log_file.write(full_message)
        except Exception as e:
            print(f"Error writing to log file during init: {str(e)}")
        
        # Update UI if available
        if hasattr(self, 'log_area') and self.log_area is not None:
            try:
                self.log_area.configure(state='normal') # Make writable
                self.log_area.insert(tk.END, full_message)
                self.log_area.see(tk.END)
                self.log_area.configure(state='disabled') # Make readonly again
            except tk.TclError: # Handle case where widget might be destroyed
                pass

    def log_message(self, message):
        """Log a message to the text area with timestamp"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        full_message = f"[{timestamp}] {message}\n"

        # Update the UI if available
        if hasattr(self, 'log_area') and self.log_area is not None:
            try:
                 # Ensure updates happen in the main thread
                self.root.after(0, lambda msg=full_message: self._update_log_area(msg))
            except RuntimeError: # Handle case where root window might be destroyed
                 print(full_message.strip()) # Fallback to console
        else:
            print(full_message.strip()) # Log to console if UI not ready

        # Also save to log file
        try:
            with open(self.log_file, "a", encoding='utf-8') as log_file:
                log_file.write(full_message)
        except Exception as e:
            print(f"Error writing to log file: {str(e)}")

    def _update_log_area(self, message):
        """Helper to update log area safely from main thread"""
        if hasattr(self, 'log_area') and self.log_area:
             try:
                 self.log_area.configure(state='normal') # Make writable
                 self.log_area.insert(tk.END, message)
                 self.log_area.see(tk.END) # Scroll to the end
                 self.log_area.configure(state='disabled') # Make readonly again
             except tk.TclError: # Handle case where widget might be destroyed
                  pass

    def load_settings(self):
        """Load settings from JSON file"""
        # Define defaults including new BLF paths
        default_settings = {
            'batpack_id': 'TestPack001',
            'cdp_path': r"C:\00_BTF_Projects\01_Scalexio_Projects\03_BTF_DS_EMA_Prj\Instrumentation\CLD_Prj_EMA_IPB2_NCR24Q2_v24\CLD_Prj_EMA_IPB2_NCR24Q2_v24.CDP",
            'experiment_name': 'BTF_Experiment',
            'controldesk_visible': True,
            'auto_record': False,
            'blf_pmz_csv_path': '',
            'blf_debug_csv_path': ''
        }
        try:
            if os.path.exists(self.settings_file):
                backup_file = f"{self.settings_file}.bak"
                try:
                    import shutil
                    shutil.copy2(self.settings_file, backup_file)
                except Exception as backup_error:
                    self.safe_log_message(f"Could not create settings backup: {str(backup_error)}")

                try:
                    with open(self.settings_file, 'r', encoding='utf-8') as f:
                        loaded_settings = json.load(f)

                    merged_settings = default_settings.copy()
                    merged_settings.update(loaded_settings)

                    # Migrate backend URL to gemini-dash
                    url = merged_settings.get('backend_api_url', '')
                    if not url or '10.226.38.100' in url or (url.rstrip('/').endswith('jlr-apps.com') and '/api' not in url):
                        merged_settings['backend_api_url'] = 'https://gemini-dash.jlr-apps.com/api'

                    # Migrate: default environment to prod
                    if merged_settings.get('ddg_environment', '') == 'dev':
                        merged_settings['ddg_environment'] = 'prod'

                    self.safe_log_message("Settings loaded successfully")
                    return merged_settings
                except json.JSONDecodeError:
                    self.safe_log_message("Error: Settings file is corrupted, attempting recovery...")
                    if os.path.exists(backup_file):
                        try:
                            with open(backup_file, 'r', encoding='utf-8') as f:
                                backup_settings = json.load(f)
                            merged_settings = default_settings.copy()
                            merged_settings.update(backup_settings)
                            self.safe_log_message("Settings restored from backup")
                            with open(self.settings_file, 'w', encoding='utf-8') as f:
                                json.dump(merged_settings, f, indent=4)
                            return merged_settings
                        except Exception as restore_err:
                            self.safe_log_message(f"Backup settings also corrupted ({restore_err}), using defaults")
                    else:
                        self.safe_log_message("No backup found, using defaults")

                    with open(self.settings_file, 'w', encoding='utf-8') as f:
                        json.dump(default_settings, f, indent=4)
                    return default_settings
            else:
                self.safe_log_message("Settings file not found, creating with defaults")
                with open(self.settings_file, 'w', encoding='utf-8') as f:
                    json.dump(default_settings, f, indent=4)
                return default_settings
        except Exception as e:
            self.safe_log_message(f"Critical error loading settings: {str(e)}")
            return default_settings

    def save_settings(self):
        """Save settings to JSON file"""
        try:
            # Update settings with current values from UI - ADD BLF paths
            self.settings['batpack_id'] = self.batpack_id.get()
            self.settings['cdp_path'] = self.cdp_path.get()
            self.settings['experiment_name'] = self.exp_name.get()
            self.settings['controldesk_visible'] = self.cd_visible_var.get()
            self.settings['auto_record'] = self.auto_record_var.get()
            # Ensure these vars exist before getting
            if hasattr(self, 'blf_pmz_csv_path_var'):
                 self.settings['blf_pmz_csv_path'] = self.blf_pmz_csv_path_var.get()
            if hasattr(self, 'blf_debug_csv_path_var'):
                 self.settings['blf_debug_csv_path'] = self.blf_debug_csv_path_var.get()
            # GCS upload and BLF interval settings
            if hasattr(self, 'blf_save_interval_var'):
                 self.settings['blf_save_interval'] = int(self.blf_save_interval_var.get())
            if hasattr(self, 'test_name_var'):
                 self.settings['test_name'] = self.test_name_var.get()
            if hasattr(self, 'ddg_environment_var'):
                 self.settings['ddg_environment'] = self.ddg_environment_var.get()
            if hasattr(self, 'test_setup_var'):
                 self.settings['test_setup'] = self.test_setup_var.get()
            if hasattr(self, 'sample_no_var'):
                 self.settings['sample_no'] = self.sample_no_var.get()
            if hasattr(self, 'backend_api_url_var'):
                 self.settings['backend_api_url'] = self.backend_api_url_var.get()
            if hasattr(self, 'auto_upload_blf_var'):
                 self.settings['auto_upload_blf'] = self.auto_upload_blf_var.get()
            # CAN Streaming settings
            if hasattr(self, 'stream_rig_id_var'):
                self.settings['stream_rig_id'] = self.stream_rig_id_var.get()
            if hasattr(self, 'stream_ip_var'):
                self.settings['stream_scalexio_ip'] = self.stream_ip_var.get()
            if hasattr(self, 'stream_fd_var'):
                self.settings['stream_can_fd'] = self.stream_fd_var.get()
            if hasattr(self, 'stream_pubsub_project_var'):
                self.settings['stream_pubsub_project'] = self.stream_pubsub_project_var.get()
            if hasattr(self, 'stream_pubsub_topic_var'):
                self.settings['stream_pubsub_topic'] = self.stream_pubsub_topic_var.get()

            # Write to a temporary file first
            temp_file = f"{self.settings_file}.tmp"
            with open(temp_file, 'w', encoding='utf-8') as f:
                # Use pretty-printing with indentation for better readability
                json.dump(self.settings, f, indent=4)

            # If temporary file was written successfully, rename it to the actual settings file
            backup_file = f"{self.settings_file}.bak"
            if os.path.exists(self.settings_file):
                try:
                    os.replace(self.settings_file, backup_file) # More atomic replace/backup
                except Exception as e:
                    self.log_message(f"Warning: Could not create settings backup via replace: {str(e)}")
                    # Fallback to rename if replace fails (less atomic)
                    try:
                         os.rename(self.settings_file, backup_file)
                    except Exception as e2:
                         self.log_message(f"Warning: Could not create settings backup via rename: {str(e2)}")


            # Rename temp file to actual settings file
            os.replace(temp_file, self.settings_file)

            # self.log_message("Settings saved successfully") # Reduce noise
        except Exception as e:
            self.log_message(f"Error saving settings: {str(e)}")

    def set_batpack_id(self):
        """Set BatPack ID in ControlDesk and save it to settings"""
        batpack_id = self.batpack_id.get()

        if not batpack_id.strip():
            messagebox.showerror("Error", "BatPack ID cannot be empty")
            return

        # Save the ID to settings immediately
        self.settings['batpack_id'] = batpack_id
        self.save_settings()

        if self.controldesk:
            try:
                if hasattr(self.controldesk, 'set_batpack_id'):
                     self.controldesk.set_batpack_id(batpack_id)
                     self.log_message(f"BatPack ID set to: {batpack_id}")
                else:
                     self.log_message("ControlDesk object does not have 'set_batpack_id' method.")
            except Exception as e:
                self.log_message(f"Error setting BatPack ID: {str(e)}")
                messagebox.showerror("Error", f"Failed to set BatPack ID:\n{str(e)}")
        else:
            self.log_message(f"BatPack ID saved: {batpack_id} (will be applied when Control Desk starts)")

    def on_batpack_id_change(self, event=None):
        """Save BatPack ID when it changes"""
        # Prevent recursive calls if set programmatically
        if self.updating_id:
            return

        new_id = self.batpack_id.get()
        # Only save and log if the value actually changed from settings perspective
        if self.settings.get('batpack_id') != new_id:
             self.settings['batpack_id'] = new_id
             self.save_settings()

             # If ControlDesk is running, update the BatPack ID there too
             if self.controldesk and hasattr(self.controldesk, 'bus_recorder') and hasattr(self.controldesk.bus_recorder, 'set_batpack_id'):
                 self.controldesk.bus_recorder.set_batpack_id(new_id)

             self.log_message(f"BatPack ID updated to: {new_id}")

    def browse_cdp_file(self):
        """Open file browser to select a CDP file"""
        
        # original_state = 'readonly' # Initialize original_state - MOVED
        # Flag to track if we entered edit mode within this function
        entered_edit_mode_here = False 
        
        # If we're in readonly mode, first enable editing
        if not self.edit_paths_var.get():
            self.toggle_path_editing()  # This will set both path and experiment name to editable
            entered_edit_mode_here = True # Mark that we enabled editing
            # After selecting, we'll automatically toggle back if we entered edit mode here
        else:
            # Already in edit mode, just temporarily ensure the path entry is normal
            original_state = self.cdp_path_entry.cget('state')
            self.cdp_path_entry.config(state='normal')
        
        initial_dir_cdp = os.path.dirname(self.cdp_path.get()) if self.cdp_path.get() else os.path.expanduser("~")
        filepath = filedialog.askopenfilename(
            title="Select ControlDesk Project File",
            filetypes=[("ControlDesk Project", "*.CDP"), ("All Files", "*.*")],
            initialdir=initial_dir_cdp
        )

        # Initialize original_state here to ensure it's defined before the check
        original_state = 'readonly' # Default value if not set in 'else' block below
        if not entered_edit_mode_here: # If we started in edit mode, we need the captured state
            # This assumes the entry exists and has a state property
            try:
                original_state = self.cdp_path_entry.cget('state')
            except tk.TclError:
                # Handle case where widget might not exist yet (unlikely here)
                pass 

        # Restore original state if needed (only if browsing happened in edit mode)
        # This logic seems intended to run if the user CANCELS the dialog while in edit mode
        if self.edit_paths_var.get() and original_state != 'normal':
             self.cdp_path_entry.config(state=original_state)

        
        if filepath:
            self.cdp_path.set(filepath)
            self.on_cdp_path_change() # Will save and update experiment list

            # If we entered edit mode specifically for this browse action,
            # toggle back to save/readonly state automatically.
            if entered_edit_mode_here:
                self.toggle_path_editing() # This will trigger the save/readonly part
            
    def on_cdp_path_change(self, event=None):
        """Save ControlDesk project path when it changes"""
        new_path = self.cdp_path.get()
        # Only save and log if the value actually changed from settings perspective
        if self.settings.get('cdp_path') != new_path:
             self.settings['cdp_path'] = new_path
             self.save_settings()
             self.log_message(f"ControlDesk project path updated")

             # Update the experiment list based on the new CDP path
             self.update_experiment_list()

        # If this was called from the edit button turning off editing,
        # make sure the fields are set to readonly
        if event is None and not self.edit_paths_var.get():
             self.cdp_path_entry.config(state='readonly')
             self.exp_name_entry.config(state='readonly')
             self.exp_name_entry.configure(background=self.readonly_bg)


    def on_exp_name_change(self, event=None):
        """Save experiment name when it changes"""
        new_exp = self.exp_name.get()
        # Only save and log if the value actually changed from settings perspective
        if self.settings.get('experiment_name') != new_exp:
            self.settings['experiment_name'] = new_exp
            self.save_settings()
            self.log_message(f"Experiment name updated to: {new_exp}")

        # If this was called from the edit button turning off editing,
        # make sure the fields are set to readonly
        if event is None and not self.edit_paths_var.get():
            self.cdp_path_entry.config(state='readonly')
            self.exp_name_entry.config(state='readonly')
            self.exp_name_entry.configure(background=self.readonly_bg)

    def load_logs(self):
        """Load existing logs from the log file"""
        try:
            if os.path.exists(self.log_file):
                with open(self.log_file, 'r', encoding='utf-8') as f:
                    # Read last N lines (e.g., 100)
                    lines = f.readlines()[-100:]
                    # Only insert logs if the log_area exists and has content method
                    if hasattr(self, 'log_area') and self.log_area is not None and hasattr(self.log_area, 'insert'):
                        self.log_area.configure(state='normal') # Ensure writable
                        for line in lines:
                            self.log_area.insert(tk.END, line)
                        self.log_area.configure(state='disabled') # Set back to readonly
                        self.log_area.see(tk.END)  # Scroll to the end
                self.safe_log_message("Previous logs loaded")
            else:
                self.safe_log_message("New log session started")
        except Exception as e:
            error_msg = f"Error loading logs: {str(e)}"
            print(error_msg)
            if hasattr(self, 'log_area') and self.log_area is not None and hasattr(self.log_area, 'insert'):
                try:
                    self.log_area.configure(state='normal')
                    self.log_area.insert(tk.END, f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {error_msg}\n")
                    self.log_area.configure(state='disabled')
                    self.log_area.see(tk.END)
                except tk.TclError: pass # Ignore if widget destroyed

    def clear_log(self):
        """Clear both the log file and display"""
        if messagebox.askyesno("Clear Log", "Are you sure you want to clear all logs? This cannot be undone."):
            try:
                # Clear file
                with open(self.log_file, 'w', encoding='utf-8') as f:
                    f.write("") # Write empty string to clear
                # Clear display
                if hasattr(self, 'log_area') and self.log_area:
                     self.log_area.configure(state='normal')
                     self.log_area.delete(1.0, tk.END)
                     self.log_area.configure(state='disabled')
                self.log_message("Log cleared")
            except Exception as e:
                messagebox.showerror("Error", f"Failed to clear log: {str(e)}")

    def on_closing(self):
        """Handle window close event - save settings and clean up"""
        # Show confirmation dialog
        if not messagebox.askyesno("Confirm Exit", "Are you sure you want to close the application?"):
            return

        # Stop CAN streaming if active
        if self.stream_manager and self.stream_manager.is_streaming:
            self.log_message("[CAN Stream] Stopping streaming before exit...")
            self.stream_manager.stop_streaming()

        # Wait for pending GCS uploads before closing
        if self.controldesk and hasattr(self.controldesk, 'bus_recorder') and self.controldesk.bus_recorder:
            self.controldesk.bus_recorder.wait_for_uploads(timeout=60)

        # Save current settings
        self._save_stream_settings()
        self.save_settings()

        # Gracefully handle ControlDesk closing
        if self.controldesk:
            try:
                # Turn off VBatt if it's on
                if hasattr(self, 'vbatt_state') and self.vbatt_state.get():
                    try:
                        self.log_message("Turning off VBatt before exit...")
                        self._update_vbatt_button_state(False) # Update UI immediately
                        if hasattr(self.controldesk, 'set_vbatt_state'):
                            self.controldesk.set_vbatt_state(0)
                            self.log_message("VBatt turned off")
                    except Exception as e:
                        self.log_message(f"Warning: Error turning off VBatt: {str(e)}")

                # Ask user about closing Control Desk
                if messagebox.askyesno("Confirm", "Control Desk is still running. Close it?"):
                    self.log_message("Closing Control Desk before exit...")

                    # Show progress dialog
                    progress_window = tk.Toplevel(self.root)
                    progress_window.title("Closing ControlDesk")
                    progress_window.geometry("300x100")
                    progress_window.transient(self.root)
                    progress_window.grab_set()
                    progress_window.protocol("WM_DELETE_WINDOW", lambda: None) # Prevent closing dialog

                    # Center the window
                    try:
                        x = self.root.winfo_rootx() + self.root.winfo_width()//2 - 150
                        y = self.root.winfo_rooty() + self.root.winfo_height()//2 - 50
                        progress_window.geometry(f"+{x}+{y}")
                    except: pass # Ignore geometry errors if root window gone

                    message_label = ttk.Label(progress_window, text="Waiting for ControlDesk to close...\nThis may take a few moments.")
                    message_label.pack(pady=10)
                    progress_bar = ttk.Progressbar(progress_window, mode="indeterminate")
                    progress_bar.pack(fill=tk.X, padx=20, pady=10)
                    progress_bar.start()

                    # Function to handle ControlDesk closing in a thread
                    def close_controldesk_and_wait_thread():
                        closed_ok = False
                        try:
                            if hasattr(self.controldesk, 'close') and callable(self.controldesk.close):
                                closed_ok = self.controldesk.Quit() # Assume close waits or returns status

                            # Optional: Add a small delay/check if needed after close returns
                            # time.sleep(1)

                        except Exception as e:
                            self.log_message(f"Error during ControlDesk shutdown thread: {str(e)}")
                        finally:
                             # Update UI from main thread regardless of success
                            self.root.after(0, lambda ok=closed_ok: self._finish_closing(progress_window, ok))

                    # Start the closing thread
                    close_thread = threading.Thread(target=close_controldesk_and_wait_thread, daemon=True)
                    close_thread.start()

                    # Prevent the main window from closing yet
                    return
                else:
                    self.log_message("Control Desk left running on application exit")

            except Exception as e:
                self.log_message(f"Error handling ControlDesk during shutdown: {str(e)}")

        # If ControlDesk wasn't running or user chose not to close it
        self.log_message("Application closed by user")
        self.root.destroy()

        # Stop disk space update timer when closing
        if self.disk_space_timer_id:
            self.root.after_cancel(self.disk_space_timer_id)
            self.disk_space_timer_id = None

    def _finish_closing(self, progress_window, closed_ok):
        """Complete the closing process after attempting ControlDesk close"""
        try:
            progress_window.destroy()
        except tk.TclError: pass # Ignore if already destroyed

        if closed_ok:
            self.log_message("ControlDesk closed successfully.")
        else:
            self.log_message("ControlDesk may not have closed properly or was already closed.")

        # Reset controldesk reference
        self.controldesk = None

        # Stop periodic updates if they were running
        self.stop_periodic_updates()

        self.log_message("Shutting down application")
        self.root.destroy()

    def launch_controldesk(self):
        """Launch ControlDesk application in a separate thread"""
        if self.controldesk_thread and self.controldesk_thread.is_alive():
            self.log_message("Control Desk launch/close already in progress or running.")
            return

        # Check availability
        if not CONTROLDESK_AVAILABLE:
             messagebox.showerror("Error", "ControlDesk module not found. Cannot launch.")
             return

        # Disable launch button to prevent double-clicking
        self.cd_start_button.config(state=tk.DISABLED)

        # Get paths from settings
        cdp_path = self.cdp_path.get()
        experiment_name = self.exp_name.get()

        # Check if the CDP file exists
        if not os.path.exists(cdp_path):
            self.log_message(f"Error: ControlDesk project file not found at: {cdp_path}")
            messagebox.showerror("File Not Found", f"ControlDesk project file not found:\n{cdp_path}")
            self.cd_start_button.config(state=tk.NORMAL) # Re-enable button
            return

        # Get visibility setting
        visible = self.cd_visible_var.get()
        mode_text = "visible" if visible else "invisible"
        self.log_message(f"Launching Control Desk ({mode_text} mode)...")

        # Start ControlDesk in a separate thread (using the worker)
        self.controldesk_thread = threading.Thread(
            target=self._auto_start_controldesk_worker, # Reusing the auto-start worker
            args=(cdp_path, experiment_name, visible),
            daemon=True
        )
        self.controldesk_thread.start()

        # Enable Close button (optimistically, worker thread updates status)
        self.cd_close_button.config(state=tk.NORMAL)

    def close_controldesk(self):
        """Close the dSpace Control Desk application"""
        if not self.controldesk:
            self.log_message("Control Desk is not running")
            return

        if self.controldesk_thread and self.controldesk_thread.is_alive():
            self.log_message("Control Desk launch/close already in progress.")
            return

        # Show confirmation dialog
        if not messagebox.askyesno("Confirm", "Are you sure you want to close Control Desk?"):
            self.log_message("Control Desk close cancelled by user")
            return

        self.log_message("Closing dSpace Control Desk...")

        # Disable close button immediately
        self.cd_close_button.config(state=tk.DISABLED)
        # Optionally disable start button too
        # self.cd_start_button.config(state=tk.DISABLED)

        try:
            # Turn off VBatt first if it's on
            if hasattr(self, 'vbatt_state') and self.vbatt_state.get():
                try:
                    self.log_message("Turning off VBatt before closing...")
                    self._update_vbatt_button_state(False) # Update UI immediately
                    if hasattr(self.controldesk, 'set_vbatt_state'):
                        self.controldesk.set_vbatt_state(0)
                        self.log_message("VBatt turned off")
                except Exception as e:
                    self.log_message(f"Warning: Error turning off VBatt: {str(e)}")

            # Stop periodic updates before closing
            self.stop_periodic_updates()

            # Show progress dialog
            progress_window = tk.Toplevel(self.root)
            progress_window.title("Closing ControlDesk")
            progress_window.geometry("300x100")
            progress_window.transient(self.root)
            progress_window.grab_set()
            progress_window.protocol("WM_DELETE_WINDOW", lambda: None)

            try:
                x = self.root.winfo_rootx() + self.root.winfo_width()//2 - 150
                y = self.root.winfo_rooty() + self.root.winfo_height()//2 - 50
                progress_window.geometry(f"+{x}+{y}")
            except: pass

            message_label = ttk.Label(progress_window, text="Waiting for ControlDesk to close...")
            message_label.pack(pady=10)
            progress_bar = ttk.Progressbar(progress_window, mode="indeterminate")
            progress_bar.pack(fill=tk.X, padx=20, pady=10)
            progress_bar.start()


            # Create a separate thread for closing to avoid freezing the GUI
            def close_cd_thread():
                closed_ok = False
                com_initialized = False
                try:
                    # Make sure to initialize COM in this thread
                    import pythoncom
                    pythoncom.CoInitialize()
                    com_initialized = True

                    if hasattr(self.controldesk, 'close') and callable(self.controldesk.close):
                        closed_ok = self.controldesk.Quit() # Assume close waits or returns status
                        if closed_ok:
                             self.root.after(0, lambda: self.log_message("ControlDesk close method returned True"))
                        else:
                             self.root.after(0, lambda: self.log_message("ControlDesk close method returned False"))

                except Exception as e:
                    self.root.after(0, lambda: self.log_message(f"Error during close method: {str(e)}"))
                finally:
                    # Always uninitialize COM if initialized
                    if com_initialized:
                        try:
                            pythoncom.CoUninitialize()
                        except: pass # Ignore errors during uninit

                # Update GUI from main thread
                self.root.after(0, lambda ok=closed_ok: self._handle_controldesk_closed(progress_window, ok))

            # Start the thread
            self.controldesk_thread = threading.Thread(target=close_cd_thread, daemon=True)
            self.controldesk_thread.start()

        except Exception as e:
            self.log_message(f"Error initiating Control Desk close: {str(e)}")
            self._handle_controldesk_closed(None, False) # Handle cleanup even if thread fails

    def _handle_controldesk_closed(self, progress_window, closed_ok):
        """Update UI after ControlDesk has been closed"""
        # Close progress window if it exists
        if progress_window:
             try:
                 progress_window.destroy()
             except tk.TclError: pass

        # Update UI elements
        self.cd_status.config(text="Not Running", foreground="red")
        self.cd_start_button.config(state=tk.NORMAL) # Re-enable start
        self.cd_close_button.config(state=tk.DISABLED)
        self.bus_record_button.config(state=tk.DISABLED)

        # Reset the controldesk object
        self.controldesk = None
        self.controldesk_thread = None # Reset thread tracker

        # Reset real-time values
        self.recorder_counter_value.config(text="0")
        self.time_recorded_value.config(text="00:00:00")
        self.recording_status_value.config(text="Idle", foreground="blue")
        if hasattr(self, 'lv_voltage_value'):
            self.lv_voltage_value.config(text="0.00 V", foreground="black")
        if hasattr(self, 'lv_current_value'):
            self.lv_current_value.config(text="0.00 A", foreground="black")

        # Reset VBatt UI
        self._update_vbatt_button_state(False)


        self.log_message("Control Desk references cleaned up")

    def _auto_start_recording(self):
        """Start bus recording automatically after delay"""
        if not self.controldesk:
            self.log_message("ControlDesk is no longer running. Cannot auto-start monitoring.")
            return

        # Check if already recording
        if hasattr(self.controldesk, 'is_recording') and self.controldesk.is_recording():
             self.log_message("Bus monitoring already active. Skipping auto-start.")
             return

        self.log_message("Auto-starting bus monitoring...")
        self.bus_record()

    def bus_record(self):
        """Run Bus_record method to record data"""
        if not self.controldesk:
            self.log_message("Control Desk is not running")
            messagebox.showwarning("Not Running", "ControlDesk is not running. Cannot start monitoring.")
            return

        # Check if already recording or about to
        if self.bus_record_button['state'] == tk.DISABLED:
             self.log_message("Bus monitoring is already active or starting.")
             return

        self.log_message("Starting Bus Monitoring...")

        # Disable the bus record button immediately
        self.bus_record_button.config(state=tk.DISABLED)
        self.recording_status_value.config(text="Starting...", foreground="orange")


        # Create and start a thread for Bus_record
        # Make sure only one recording thread runs
        if hasattr(self, 'bus_record_thread') and self.bus_record_thread and self.bus_record_thread.is_alive():
            self.log_message("Bus monitoring thread is already running.")
            # Maybe re-enable button if thread is stuck? Risky.
            # self.bus_record_button.config(state=tk.NORMAL)
            return

        self.bus_record_thread = threading.Thread(target=self._run_bus_record, daemon=True)
        self.bus_record_thread.start()

    def _run_bus_record(self):
        """Run the Bus_record operation in a separate thread"""
        try:
            # Check if the controldesk object has the Bus_record method (Note the capitalization)
            if hasattr(self.controldesk, 'Bus_record'):
                batpack_id = self.batpack_id.get()
                # Pass BLF save interval to ControlDesk before starting
                try:
                    interval = int(self.blf_save_interval_var.get())
                    self.controldesk.set_file_max_time(interval)
                except (ValueError, AttributeError):
                    pass
                # Pass GCS upload settings to bus recorder
                if self.controldesk.bus_recorder:
                    self.controldesk.bus_recorder.auto_upload_blf = self.auto_upload_blf_var.get()
                    self.controldesk.bus_recorder.test_name = self.test_name_var.get()
                    self.controldesk.bus_recorder.test_setup = self.test_setup_var.get()
                    self.controldesk.bus_recorder.sample_no = self.sample_no_var.get()
                    self.controldesk.bus_recorder.ddg_environment = self.ddg_environment_var.get()
                    self.controldesk.bus_recorder.backend_api_url = self.backend_api_url_var.get()
                    self.controldesk.bus_recorder.api_token = self.settings.get('api_token', '')
                # Call Bus_record, assuming it handles its own state updates via callback
                self.controldesk.Bus_record(batpack_id, update_callback=self.update_realtime_values)
                # Note: Bus_record might block or run indefinitely if not designed carefully
                # Consider adding logic here or in Bus_record to handle stopping
                self.log_message("Bus monitoring process finished or stopped.")
                # Re-enable button after process finishes (if it ever does)
                self.root.after(0, lambda: self.bus_record_button.config(state=tk.NORMAL))
                self.root.after(0, lambda: self.recording_status_value.config(text="Idle", foreground="blue"))

            else:
                self.root.after(0, lambda: self.log_message("Error: ControlDesk object missing 'Bus_record' method."))
                self.root.after(0, lambda: self.bus_record_button.config(state=tk.NORMAL))
                self.root.after(0, lambda: self.recording_status_value.config(text="Error", foreground="red"))

        except Exception as e:
            error_msg = f"Error in Bus Record thread: {str(e)}"
            print(error_msg)
            self.root.after(0, lambda: self.log_message(error_msg))
            # Re-enable the button on error
            self.root.after(0, lambda: self.bus_record_button.config(state=tk.NORMAL))
            self.root.after(0, lambda: self.recording_status_value.config(text="Error", foreground="red"))

    def on_window_resize(self, event):
        """Handle window resize event to update logo size"""
        try:
            # Check if logo_label exists
            if not hasattr(self, 'logo_label') or not self.logo_label.winfo_exists():
                return

            # Get the path to the logo file
            logo_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logo.jpg")

            if os.path.exists(logo_path):
                # Load original image
                original_image = Image.open(logo_path)
                width, height = original_image.size

                # Calculate available width based on the logo_frame's current width
                frame_width = self.logo_frame.winfo_width()
                available_width = max(frame_width - 10, 10) # Subtract some padding, ensure min width

                # Calculate new height maintaining aspect ratio
                new_width = available_width
                new_height = int(height * (new_width / width))

                # Resize only if dimensions are valid
                if new_width > 0 and new_height > 0:
                    resized_image = original_image.resize((new_width, new_height), Image.LANCZOS)

                    # Convert to PhotoImage
                    # Keep a reference to prevent garbage collection
                    self.resized_logo_image = ImageTk.PhotoImage(resized_image)

                    # Update the logo label with the new image
                    self.logo_label.config(image=self.resized_logo_image)

        except Exception as e:
            # Ignore errors during resize (e.g., if window is minimized)
            # print(f"Error resizing logo: {str(e)}")
            pass

    def toggle_path_editing(self):
        """Toggle the editability of the project path and experiment name fields"""
        current_state = self.edit_paths_var.get()
        new_state = not current_state
        self.edit_paths_var.set(new_state)

        if new_state:
            # Enable editing
            self.cdp_path_entry.config(state='normal')
            self.exp_name_entry.config(state='normal')
            # Set background to white to indicate editable state
            self.exp_name_entry.configure(background=self.normal_bg)
            self.edit_paths_button.config(text="Save")
            self.log_message("Path editing enabled")
        else:
            # Save changes (on_cdp/exp_change methods handle saving)
            # Trigger save explicitly if needed, but binding should cover it
            self.on_cdp_path_change(event=None) # Pass event=None to trigger save logic
            self.on_exp_name_change(event=None) # Pass event=None to trigger save logic

            # Disable editing
            self.cdp_path_entry.config(state='readonly')
            self.exp_name_entry.config(state='readonly')
            # Set background to gray to indicate readonly state
            self.exp_name_entry.configure(background=self.readonly_bg)
            self.edit_paths_button.config(text="Edit")
            # self.log_message("Path changes saved (if modified)") # Reduce noise

    def apply_settings_to_ui(self):
        """Apply loaded settings to all UI elements"""
        try:
            # Use self.updating_id to prevent save triggers during initial load
            self.updating_id = True

            # Apply settings to BatPack ID
            if 'batpack_id' in self.settings:
                self.batpack_id.set(self.settings['batpack_id'])

            # Apply settings to CDP path
            if 'cdp_path' in self.settings:
                self.cdp_path.set(self.settings['cdp_path'])
            # Ensure readonly state if not editing
            if not self.edit_paths_var.get():
                 self.cdp_path_entry.config(state='readonly')

            # Apply settings to experiment name
            if 'experiment_name' in self.settings:
                self.exp_name.set(self.settings['experiment_name'])
            # Ensure readonly state and style if not editing
            if not self.edit_paths_var.get():
                 self.exp_name_entry.config(state='readonly')
                 self.exp_name_entry.configure(background=self.readonly_bg)
            self.update_experiment_list() # Refresh list based on loaded CDP path

            # Apply settings to auto-record checkbox
            if 'auto_record' in self.settings and hasattr(self, 'auto_record_var'):
                self.auto_record_var.set(self.settings['auto_record'])

            # Apply settings to visibility checkbox
            if 'controldesk_visible' in self.settings and hasattr(self, 'cd_visible_var'):
                self.cd_visible_var.set(self.settings['controldesk_visible'])

            # Apply settings to BLF Converter paths
            if hasattr(self, 'blf_pmz_csv_path_var'): # Check if UI element exists
                self.blf_pmz_csv_path_var.set(self.settings.get('blf_pmz_csv_path', ''))
            if hasattr(self, 'blf_debug_csv_path_var'):
                self.blf_debug_csv_path_var.set(self.settings.get('blf_debug_csv_path', ''))
            # Update the output path display based on loaded PMZ path
            self.update_output_blf_path_display()

            # Apply GCS upload and BLF interval settings
            if hasattr(self, 'blf_save_interval_var'):
                self.blf_save_interval_var.set(str(self.settings.get('blf_save_interval', 60)))
            if hasattr(self, 'test_name_var'):
                self.test_name_var.set(self.settings.get('test_name', ''))
            if hasattr(self, 'ddg_environment_var'):
                self.ddg_environment_var.set(self.settings.get('ddg_environment', 'prod'))
            if hasattr(self, 'test_setup_var'):
                self.test_setup_var.set(self.settings.get('test_setup', ''))
            if hasattr(self, 'sample_no_var'):
                self.sample_no_var.set(self.settings.get('sample_no', ''))
            if hasattr(self, 'backend_api_url_var'):
                self.backend_api_url_var.set(self.settings.get('backend_api_url', ''))
            if hasattr(self, 'auto_upload_blf_var'):
                self.auto_upload_blf_var.set(self.settings.get('auto_upload_blf', True))

            self.safe_log_message("Settings applied to UI elements")

        except Exception as e:
            self.safe_log_message(f"Error applying settings to UI: {str(e)}")
        finally:
             self.updating_id = False # Allow saves again

    def reload_settings(self):
        """Reload settings from file and apply them to the UI"""
        try:
            # Load settings from file
            self.settings = self.load_settings()

            # Apply to UI
            self.apply_settings_to_ui()

            # Log the reload
            self.log_message("Settings reloaded from file")
            self.log_message(f"Using CDP path: {self.cdp_path.get()}")
            self.log_message(f"Using experiment name: {self.exp_name.get()}")

            # Show a success message
            messagebox.showinfo("Settings Reloaded", "Settings have been successfully reloaded from file.")
        except Exception as e:
            error_msg = f"Error reloading settings: {str(e)}"
            self.log_message(error_msg)
            messagebox.showerror("Error", error_msg)

    def repair_settings_file(self):
        """Repair potentially corrupted settings file by recreating it with current settings"""
        if not messagebox.askyesno("Repair Settings", "This will recreate your settings file with current values. Continue?"):
            return

        try:
            # Make sure we have the latest settings from UI
            current_settings = {
                'batpack_id': self.batpack_id.get(),
                'cdp_path': self.cdp_path.get(),
                'experiment_name': self.exp_name.get(),
                'controldesk_visible': self.cd_visible_var.get(),
                'auto_record': self.auto_record_var.get(),
                'blf_pmz_csv_path': self.blf_pmz_csv_path_var.get() if hasattr(self, 'blf_pmz_csv_path_var') else '',
                'blf_debug_csv_path': self.blf_debug_csv_path_var.get() if hasattr(self, 'blf_debug_csv_path_var') else '',
                'test_name': self.test_name_var.get() if hasattr(self, 'test_name_var') else '',
                'ddg_environment': self.ddg_environment_var.get() if hasattr(self, 'ddg_environment_var') else 'prod',
                'test_setup': self.test_setup_var.get() if hasattr(self, 'test_setup_var') else '',
                'sample_no': self.sample_no_var.get() if hasattr(self, 'sample_no_var') else '',
                'backend_api_url': self.backend_api_url_var.get() if hasattr(self, 'backend_api_url_var') else '',
                'auto_upload_blf': self.auto_upload_blf_var.get() if hasattr(self, 'auto_upload_blf_var') else False,
            }

            # Create backup file name
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_file = f"{self.settings_file}.{timestamp}.bak"

            # Try to backup existing file
            if os.path.exists(self.settings_file):
                try:
                    import shutil
                    shutil.copy2(self.settings_file, backup_file)
                    self.log_message(f"Backup created at: {backup_file}")
                except Exception as e:
                    self.log_message(f"Could not create backup: {str(e)}")

            # Write new settings file
            with open(self.settings_file, 'w', encoding='utf-8') as f:
                json.dump(current_settings, f, indent=4)

            self.log_message("Settings file repaired successfully")
            messagebox.showinfo("Success", "Settings file has been repaired successfully.")

        except Exception as e:
            error_msg = f"Error repairing settings file: {str(e)}"
            self.log_message(error_msg)
            messagebox.showerror("Error", error_msg)

    def update_experiment_list(self):
        """Scan for experiment files (.cde) in the folder containing the CDP file"""
        experiment_names = []
        try:
            cdp_path = self.cdp_path.get()

            # Check if CDP path is set and exists
            if not cdp_path or not os.path.exists(cdp_path):
                self.log_message("Cannot update experiment list: CDP file not found or path not set")
                # Clear existing values if path is invalid
                if hasattr(self, 'exp_name_entry'):
                     self.exp_name_entry['values'] = []
                     self.exp_name.set("")
                return

            # Get the directory containing the CDP file
            cdp_dir = os.path.dirname(cdp_path)
            # self.log_message(f"Scanning for experiment files (.cde) in: {cdp_dir}") # Reduce verbosity

            # Store current value to restore later if possible
            current_value = self.exp_name.get()

            # Remember current state to restore later
            current_state = self.exp_name_entry.cget('state')

            # Temporarily set to normal to update values
            self.exp_name_entry.config(state='normal')
            self.exp_name_entry.configure(background=self.normal_bg)

            # Look for .cde files in this folder and subfolders (limited depth)
            try:
                # Use os.walk for recursive search
                self.log_message(f"Starting recursive scan (depth=2) in {cdp_dir}...")
                # Get starting depth for comparison
                cdp_dir_abs = os.path.abspath(cdp_dir)
                cdp_dir_depth = cdp_dir_abs.count(os.sep)

                for root, dirs, files in os.walk(cdp_dir):
                    # Calculate current depth relative to start
                    root_abs = os.path.abspath(root)
                    current_depth = root_abs.count(os.sep)
                    relative_depth = current_depth - cdp_dir_depth

                    # Limit recursion depth to 2 levels below cdp_dir
                    if relative_depth >= 2:
                        # print(f"  Reached depth limit at {root}, stopping descent.") # Debug log
                        dirs[:] = [] # Clear dirs list to prevent further descent from here

                    # self.log_message(f"  Scanning: {root} (depth {relative_depth})") # Verbose logging
                    for filename in files:
                        if filename.lower().endswith('.cde'):
                            exp_name = os.path.splitext(filename)[0]
                            if exp_name not in experiment_names: # Avoid duplicates if names are same in different folders
                                experiment_names.append(exp_name)
                                self.log_message(f"  Found experiment: {exp_name} in {root}") # Log where found
            except Exception as walk_error:
                self.log_message(f"Error during directory scan in {cdp_dir}: {str(walk_error)}")
            
            # Sort the list
            experiment_names.sort()

            # Set the combobox values
            self.exp_name_entry['values'] = experiment_names

            # Restore current value or use first in list if available
            if current_value and current_value in experiment_names:
                self.exp_name.set(current_value)
            elif experiment_names:
                self.exp_name.set(experiment_names[0])
                # self.log_message(f"Selected first available experiment: {experiment_names[0]}") # Reduce verbosity
                # Save the setting if it changed due to selection
                if self.settings.get('experiment_name') != experiment_names[0]:
                     self.settings['experiment_name'] = experiment_names[0]
                     self.save_settings()
            else:
                 # If no experiments found, clear the entry
                 self.exp_name.set("")
                 self.log_message("No experiment (.cde) files found in the project directory.")


            # Restore original state and apply appropriate background color
            if current_state == 'readonly':
                self.exp_name_entry.config(state=current_state)
                self.exp_name_entry.configure(background=self.readonly_bg)
            else:
                # Keep it normal if editing was already enabled
                pass

            # self.log_message(f"Found {len(experiment_names)} experiment(s)") # Reduce verbosity

        except Exception as e:
            self.log_message(f"Error updating experiment list: {str(e)}")
            # Ensure state is restored even on error
            if hasattr(self, 'exp_name_entry') and self.exp_name_entry:
                if not self.edit_paths_var.get():
                    self.exp_name_entry.config(state='readonly')
                    self.exp_name_entry.configure(background=self.readonly_bg)
                else:
                    self.exp_name_entry.config(state='normal')
                    self.exp_name_entry.configure(background=self.normal_bg)

    def refresh_experiment_list(self):
        """Handle the refresh button click - refresh the experiment list"""
        self.log_message("Refreshing experiment list...")
        self.update_experiment_list()
        self.log_message("Experiment list refreshed.")

    def update_realtime_values(self, recorder_counter=None, time_recorded=None, lv_voltage=None, lv_current=None):
        """Update the real-time values in the GUI (called by ControlDesk callback or timer)"""

        # Define update actions within a function to run via root.after
        def _do_update():
            # Define the specific COM error code we want to handle quietly
            RPC_E_CANTCALLOUT_ININPUTSYNCCALL = -2147417843

            try:
                # Update Recorder Counter
                if recorder_counter is not None:
                    self.recorder_counter_value.config(text=str(recorder_counter))
                    self.last_counter_value = recorder_counter
                elif hasattr(self,'controldesk') and self.controldesk and hasattr(self.controldesk, 'get_recorder_counter'):
                    try: # Wrap COM call
                        counter = self.controldesk.get_recorder_counter()
                        if counter is not None: self.recorder_counter_value.config(text=str(counter))
                    except pywintypes.com_error as e:
                        if e.hresult == RPC_E_CANTCALLOUT_ININPUTSYNCCALL:
                            pass # Silently ignore this specific error for this cycle
                        else:
                            print(f"COM Error getting recorder counter: {e}")
                    except Exception as e:
                        print(f"Error getting recorder counter: {e}")

                # Update Time Recorded and Status
                current_time_recorded = time_recorded
                if current_time_recorded is None and hasattr(self,'controldesk') and self.controldesk and hasattr(self.controldesk, 'get_time_recorded'):
                    try: # Wrap COM call
                        current_time_recorded = self.controldesk.get_time_recorded()
                    except pywintypes.com_error as e:
                        if e.hresult == RPC_E_CANTCALLOUT_ININPUTSYNCCALL:
                            pass # Silently ignore
                        else:
                            print(f"COM Error getting time recorded: {e}")
                            current_time_recorded = None # Ensure it remains None on error
                    except Exception as e:
                        print(f"Error getting time recorded: {e}")
                        current_time_recorded = None

                if current_time_recorded is not None:
                    hours = int(current_time_recorded // 3600)
                    minutes = int((current_time_recorded % 3600) // 60)
                    seconds = int(current_time_recorded % 60)
                    time_str = f"{hours:02d}:{minutes:02d}:{seconds:02d}"
                    self.time_recorded_value.config(text=time_str)
                    self.last_time_recorded = current_time_recorded

                    # Update Status based on time or explicit recording state
                    is_rec = False
                    if self.controldesk and hasattr(self.controldesk, 'is_recording'): # Check self.controldesk exists
                        is_rec = self.controldesk.is_recording()

                    if is_rec or current_time_recorded > 0: # Consider recording if time > 0 even if flag check fails
                        self.recording_status_value.config(text="Recording", foreground="green")
                        self.bus_record_button.config(state=tk.DISABLED)
                    else:
                        self.recording_status_value.config(text="Idle", foreground="blue")
                        # Only enable if ControlDesk is actually running
                        if self.controldesk:
                             self.bus_record_button.config(state=tk.NORMAL)
                        else:
                             self.bus_record_button.config(state=tk.DISABLED)

                # Update LV Voltage
                current_lv_voltage = lv_voltage
                if current_lv_voltage is None and hasattr(self,'controldesk') and self.controldesk and hasattr(self.controldesk, 'get_lv_voltage'):
                    try: # Wrap COM call
                        current_lv_voltage = self.controldesk.get_lv_voltage()
                    except pywintypes.com_error as e:
                        if e.hresult == RPC_E_CANTCALLOUT_ININPUTSYNCCALL:
                            pass # Silently ignore
                        else:
                            print(f"COM Error getting LV voltage: {e}")
                            current_lv_voltage = None
                    except Exception as e:
                        print(f"Error getting LV voltage: {e}")
                        current_lv_voltage = None

                if current_lv_voltage is not None:
                    voltage_str = f"{current_lv_voltage:.2f} V"
                    self.lv_voltage_value.config(text=voltage_str)
                    color = "green"
                    if current_lv_voltage < 11.0: color = "red"
                    elif current_lv_voltage > 15.0: color = "orange"
                    self.lv_voltage_value.config(foreground=color)

                # Update LV Current
                current_lv_current = lv_current
                if current_lv_current is None and hasattr(self,'controldesk') and self.controldesk and hasattr(self.controldesk, 'get_lv_current'):
                    try: # Wrap COM call
                        current_lv_current = self.controldesk.get_lv_current()
                    except pywintypes.com_error as e:
                        if e.hresult == RPC_E_CANTCALLOUT_ININPUTSYNCCALL:
                            pass # Silently ignore
                        else:
                            print(f"COM Error getting LV current: {e}")
                            current_lv_current = None
                    except Exception as e:
                        print(f"Error getting LV current: {e}")
                        current_lv_current = None

                if current_lv_current is not None:
                    current_str = f"{current_lv_current:.2f} A"
                    self.lv_current_value.config(text=current_str)
                    color = "green"
                    if abs(current_lv_current) > 150.0: color = "red"
                    elif abs(current_lv_current) > 100.0: color = "orange"
                    self.lv_current_value.config(foreground=color)

            except Exception as e:
                # Avoid logging errors too frequently if updates fail continuously
                # Catch general errors in the update logic itself
                print(f"Error processing real-time values UI update: {str(e)}")

        # Schedule the UI update in the main thread
        # Check if root window still exists
        if self.root and self.root.winfo_exists():
             self.root.after(0, _do_update)

    def start_periodic_updates(self):
        """Start periodic updates of the real-time values (if not using callback)"""
        # Cancel any existing timer
        if self.update_timer_id:
            self.root.after_cancel(self.update_timer_id)
            self.update_timer_id = None

        # Check if ControlDesk object has a callback mechanism preferred
        # For now, assume we always need polling if start_periodic_updates is called
        # (The callback is passed to bus_record, but general state might need polling)

        def periodic_poll():
             # Only poll if ControlDesk exists and root window exists
            if self.controldesk and self.root and self.root.winfo_exists():
                try:
                    # Call update_realtime_values without args to trigger polling
                    self.update_realtime_values()
                    # Schedule the next poll ONLY if successful
                    self.update_timer_id = self.root.after(500, periodic_poll) # Poll every 500ms
                except Exception as e:
                    self.log_message(f"Error in periodic poll: {str(e)}. Stopping polling.")
                    self.stop_periodic_updates() # Stop on error
            else:
                 # Stop polling if ControlDesk is gone or window closed
                 self.stop_periodic_updates()
                 return # Don't reschedule

        # Start the first poll
        self.log_message("Starting periodic polling for real-time values.")
        periodic_poll()

    def stop_periodic_updates(self):
        """Stop periodic updates of the real-time values"""
        if self.update_timer_id:
            try:
                 if self.root and self.root.winfo_exists():
                      self.root.after_cancel(self.update_timer_id)
                 self.update_timer_id = None
                 self.log_message("Stopped periodic polling.")
            except Exception as e:
                 print(f"Error stopping periodic updates: {e}")
                 self.update_timer_id = None # Ensure it's cleared

    def save_auto_record_setting(self):
        """Save the auto-record setting when the checkbox state changes"""
        auto_record = self.auto_record_var.get()
        # Only save if the value changed
        if self.settings.get('auto_record') != auto_record:
             self.settings['auto_record'] = auto_record
             self.save_settings()
             state_text = "enabled" if auto_record else "disabled"
             self.log_message(f"Auto-start monitoring {state_text}")

    def start_blf_conversion(self):
        """Get file paths and start the BLF conversion in a thread."""
        if not CONVERTER_AVAILABLE:
            messagebox.showerror("Error", "BLF Converter module is not available.")
            return

        pmz_csv_path = self.blf_pmz_csv_path_var.get()
        debug_csv_path = self.blf_debug_csv_path_var.get()
        sdu_csv_path = self.blf_sdu_csv_path_var.get() # <-- ADDED FOR 3-CAN MODE

        if not pmz_csv_path or not debug_csv_path:
            messagebox.showerror("Error", "Please select both PMZ and Debug CSV files.")
            return

        if not os.path.exists(pmz_csv_path):
            messagebox.showerror("Error", f"PMZ CSV file not found:\n{pmz_csv_path}")
            return
        if not os.path.exists(debug_csv_path):
            messagebox.showerror("Error", f"Debug CSV file not found:\n{debug_csv_path}")
            return

        # Generate output path based on PMZ file
        try:
            directory = os.path.dirname(pmz_csv_path)
            filename = os.path.basename(pmz_csv_path)
            # Use regex for robust replacement
            new_filename_base = re.sub(r'PMZ|Debug', 'PMZ_Debug_Merged', filename, flags=re.IGNORECASE)
            new_filename, _ = os.path.splitext(new_filename_base)
            output_blf_path = os.path.join(directory, new_filename + ".blf")
            self.blf_output_path_var.set(output_blf_path) # Update display
        except Exception as e:
            self.log_message(f"Error generating output BLF path: {e}")
            messagebox.showerror("Error", f"Could not generate output BLF path:\n{e}")
            return

        self.log_message(f"Starting BLF conversion...")
        self.log_message(f"  PMZ Input: {pmz_csv_path}")
        self.log_message(f"  Debug Input: {debug_csv_path}")
        self.log_message(f"  Output: {output_blf_path}")

        # Disable button during conversion
        self.blf_start_btn.config(state=tk.DISABLED)

        # Run conversion in a separate thread
        conversion_thread = threading.Thread(
            target=self._run_conversion_thread,
            args=(pmz_csv_path, debug_csv_path,sdu_csv_path, output_blf_path),
            daemon=True
        ) # <-- ADDED FOR 3-CAN MODE
        conversion_thread.start()

    def _run_conversion_thread(self, pmz_path, debug_path, output_path):
        """Worker thread for BLF conversion."""
        try:
            # Call the conversion function from the imported module
            # Pass the GUI's log method as the callback
            # Pass sdu_path if we are in 3-CAN mode AND the file exists
            mode = self.can_mode_var.get()
            sdu_to_pass = sdu_path if (mode == "3-CAN" and os.path.exists(sdu_path)) else None
            #success = write_blf(pmz_path, debug_path, output_path, log_callback=self.log_message)
            success = write_blf(pmz_path, debug_path, output_path, log_callback=self.log_message, sdu_can_fp=sdu_to_pass) # <-- ADDED FOR 3-CAN MODE

            # Log final status (handled by callback, but maybe add a simple GUI one too)
            if success:
                # Auto-upload to GCS if enabled
                if self.auto_upload_blf_var.get() and self.test_name_var.get():
                    self.root.after(0, lambda: self.log_message("Starting BLF upload to GCS..."))
                    upload_success = upload_blf_to_gcs(
                        blf_file_path=output_path,
                        test_name=self.test_name_var.get(),
                        test_setup=self.test_setup_var.get(),
                        sample_no=self.sample_no_var.get(),
                        environment=self.ddg_environment_var.get(),
                        backend_api_url=self.backend_api_url_var.get(),
                        log_callback=lambda msg: self.root.after(0, lambda m=msg: self.log_message(m)),
                    )
                    if upload_success:
                        self.root.after(0, lambda: self.log_message("BLF upload to GCS completed successfully"))
                    else:
                        self.root.after(0, lambda: self.log_message("BLF upload to GCS failed - check logs"))
            else:
                 pass # Keep the logic flow, conversion failed

        except Exception as e:
            # Log any unexpected error during the thread execution
            error_msg = f"Critical error during BLF conversion thread: {e}"
            import traceback
            error_msg += f"\n{traceback.format_exc()}"
            # Ensure logging happens via the main thread's logger if possible
            self.root.after(0, lambda: self.log_message(error_msg))
            self.root.after(0, lambda: messagebox.showerror("Critical Error", "An unexpected error occurred during conversion. Check logs."))

        finally:
            # Re-enable button in the main thread
            self.root.after(0, lambda: self.blf_start_btn.config(state=tk.NORMAL))

    def update_output_blf_path_display(self, *args):
        """Update the display label for the output BLF path based on PMZ input."""
        pmz_path = self.blf_pmz_csv_path_var.get()
        if not pmz_path or not os.path.exists(pmz_path):
            self.blf_output_path_var.set("")
            return

        try:
            directory = os.path.dirname(pmz_path)
            filename = os.path.basename(pmz_path)

            # Use regex to replace PMZ or Debug, handling potential variations
            # Case-insensitive replacement
            new_filename_base = re.sub(r'PMZ|Debug', 'PMZ_Debug_Merged', filename, flags=re.IGNORECASE)

            # Change extension to .blf
            new_filename, _ = os.path.splitext(new_filename_base)
            new_filename += ".blf"

            output_path = os.path.join(directory, new_filename)
            self.blf_output_path_var.set(output_path)
        except Exception as e:
            self.log_message(f"Error generating output path display: {e}")
            self.blf_output_path_var.set("Error generating path")

    def browse_csv_file(self, path_var, file_type):
        """Open file dialog to select a CSV file."""
        initial_dir = os.path.dirname(path_var.get()) if path_var.get() else os.path.expanduser("~")
        filepath = filedialog.askopenfilename(
            title=f"Select {file_type} CAN CSV File",
            filetypes=[("CSV Files", "*.csv"), ("All Files", "*.*")],
            initialdir=initial_dir
        )
        if filepath:
            path_var.set(filepath)
            # Save the selected path to settings
            if file_type == "PMZ":
                self.settings['blf_pmz_csv_path'] = filepath
            elif file_type == "Debug":
                self.settings['blf_debug_csv_path'] = filepath
            self.save_settings()
            self.log_message(f"{file_type} CSV file selected: {filepath}")
            self.update_output_blf_path_display() # ADDED CALL HERE

    def update_disk_space_display(self):
        """Updates the C: drive disk space display and progress bar."""
        try:
            usage = shutil.disk_usage("C:\\")
            total_gb = usage.total / (1024**3)
            used_gb = usage.used / (1024**3)
            free_gb = usage.free / (1024**3)

            percent_used = (usage.used / usage.total) * 100
            self.disk_space_progress['value'] = percent_used
            self.disk_space_value_label.config(text=f"{free_gb:.1f} GB Free")

            # Check if free space is less than 6 GB
            if free_gb < 6:
                self.disk_space_progress.config(style="Red.Horizontal.TProgressbar")
                self.disk_space_value_label.config(foreground="red")
            else:
                self.disk_space_progress.config(style="Horizontal.TProgressbar") # Default style
                self.disk_space_value_label.config(foreground="black") # Default color

        except Exception as e:
            self.log_message(f"Error getting disk space: {e}")
            self.disk_space_value_label.config(text="Error")
            self.disk_space_progress['value'] = 0

        # Schedule next update (e.g., every 60 seconds)
        self.disk_space_timer_id = self.root.after(60000, self.update_disk_space_display)

    def get_c_drive_free_space_gb(self):
        """Returns the free space on C: drive in GB."""
        try:
            usage = shutil.disk_usage("C:\\")
            return usage.free / (1024**3)
        except Exception as e:
            self.log_message(f"Error getting C: drive free space: {e}")
            return float('inf') # Assume ample space if check fails to avoid blocking user

    def create_log_area(self):
        """Creates the log area widgets below the notebook."""
        # --- Log Area UI Elements ---
        self.log_label = ttk.Label(self.root, text="Operation Log:")
        self.log_label.grid(row=1, column=0, sticky=tk.W, pady=(10,0), padx=5)

        self.log_frame = ttk.Frame(self.root)
        self.log_frame.grid(row=2, column=0, sticky=(tk.W, tk.E, tk.N, tk.S), pady=(0, 10), padx=5)
        self.root.grid_rowconfigure(2, weight=1) # Make log area row expandable in root
        self.root.grid_columnconfigure(0, weight=1) # Make log area column expandable

        self.scrollbar = ttk.Scrollbar(self.log_frame)
        self.scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

        self.log_area = tk.Text(self.log_frame, height=10, width=70, yscrollcommand=self.scrollbar.set, state='disabled')
        self.log_area.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        self.scrollbar.config(command=self.log_area.yview)

# <-- ADDED FOR 3-CAN MODE
    def on_can_mode_change(self, event=None):
        """Save CAN Mode when it changes and update ControlDesk if running""" 
        new_mode = self.can_mode_var.get()
        self.settings['can_mode'] = new_mode
        self.save_settings()
        self.log_message(f"CAN Recording Mode set to: {new_mode}")
        if self.controldesk:
            self.controldesk.set_can_mode(new_mode)

    def fetch_sbtl_tests(self):
        """Fetch SBTL test list from backend API and populate dropdown."""
        api_url = self.backend_api_url_var.get()
        if not api_url:
            api_url = 'https://gemini-dash.jlr-apps.com/api'
            self.backend_api_url_var.set(api_url)
        try:
            import requests
            token = self.settings.get('api_token', '') or os.environ.get('DDG_API_TOKEN', '')
            if not token:
                messagebox.showwarning("Warning", "API token not set.\n\nSet 'api_token' in dspace_settings.json\nor DDG_API_TOKEN environment variable.")
                return
            headers = {'Authorization': f'Token {token}'}
            url = f"{api_url}/sbtl/tests/"
            self.log_message(f"Fetching SBTL tests from: {url}")
            resp = requests.get(url, headers=headers, timeout=10, verify=False, allow_redirects=False)
            if resp.status_code in (301, 302, 307, 308):
                redirect_url = resp.headers.get('Location', '')
                self.log_message(f"Server redirected to: {redirect_url} — following with auth")
                resp = requests.get(redirect_url, headers=headers, timeout=10, verify=False)
            resp.raise_for_status()
            tests = resp.json()
            # Extract test names, sort descending (largest number = latest)
            test_names = sorted(
                [t['name'] for t in tests if 'name' in t],
                key=lambda x: int(re.search(r'\d+', x).group()) if re.search(r'\d+', x) else 0,
                reverse=True
            )
            self.sbtl_test_combo['values'] = test_names
            if test_names:
                self.test_name_var.set(test_names[0])  # Default to latest
            self.log_message(f"Fetched {len(test_names)} SBTL tests from backend")
        except Exception as e:
            self.log_message(f"Failed to fetch SBTL tests: {e}")
            messagebox.showerror("Error", f"Failed to fetch tests:\n{e}")

def main():
    root = tk.Tk()
    app = DSpaceGUI(root)
    root.mainloop()

if __name__ == "__main__":
    # Add necessary call for multiprocessing support in frozen executables
    multiprocessing.freeze_support() # <-- ADD THIS CALL

    # Add exception handling for main GUI loop if needed
    try:
        main()
    except Exception as main_err:
         print(f"Fatal error running GUI: {main_err}")
         import traceback
         traceback.print_exc()