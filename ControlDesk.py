import threading, os, sys,pytz
import time, inspect,datetime
from time import gmtime, strftime


import pythoncom
from win32com.client import Dispatch
from dspace.com import Enums
import _thread



class BusRecorder:
    """Class to handle bus recording operations with the dSpace Control Desk"""
    
    def __init__(self, application):
        """Initialize with dSpace ControlDesk application instance
        
        Args:
            application: The ControlDeskNG.Application COM object
        """
        self.Application = application
        self.fileMaxTime = 240  # minutes
        self.debug_CAN_fp = r"N:\TEST_DATA\Dspace_CAN_Logging\Debug_CAN\Logger2_Debug_CAN.csv"
        self.pmz_CAN_fp = r"N:\TEST_DATA\Dspace_CAN_Logging\PMZ_CAN\Logger2_PMZ_CAN.csv"
        self.sdu_CAN_fp=r"N:\TEST_DATA\Dspace_CAN_Logging\SDU_CAN\Logger2_SDU_CAN.csv"
        self.batpack_id = "Unknown"  # Default value
        self.update_callback = None  # Callback function for GUI updates
        self.time_diff = 0  # Time recording progress
        self.log_callback = None  # New attribute for logging
        self._is_running = False # Flag to control the main recording loop
        self._pending_uploads = []  # Track non-daemon upload threads


    
    def set_file_max_time(self, minutes):
        """Set max recording time before CSV→BLF conversion (in minutes)."""
        self.fileMaxTime = minutes
        self.log_message(f"BusRecorder: fileMaxTime set to {minutes} minutes")

    def set_batpack_id(self, batpack_id):
        """Set the BatPack ID from the GUI and update ControlDesk if possible"""
        self.batpack_id = batpack_id
        #self.log_message(f"BusRecorder: BatPack ID set to: {self.batpack_id}")
        
        # Try to update the control in the layout if it exists
        try:
            if self.Application and hasattr(self.Application, 'LayoutManagement'):
                batID_control = self.Application.LayoutManagement.ActiveLayout.Instruments.Item("BatPack_ID")
                if batID_control:
                    batID_control.Value = str(batpack_id)
                    self.log_message(f"Updated ControlDesk BatPack_ID control to: {batpack_id}")
        except Exception as e:
            self.log_message(f"Could not update ControlDesk BatPack_ID control: {str(e)}")
    
    def set_update_callback(self, callback_func):
        """Set callback function for real-time updates
        
        The callback should accept three parameters:
        - recorder_counter: Current counter value
        - time_recorded: Current recording time in seconds
        - lv_voltage: Current LV voltage value
        - lv_current: 
        """
        self.update_callback = callback_func
    
    def set_log_callback(self, log_callback):
        """Set a callback function for sending log messages to the GUI"""
        self.log_callback = log_callback
        
    def log_message(self, message):
        """Log a message using the callback if available, otherwise print it"""
        if self.log_callback:
            self.log_callback(message)
        else:
            print(message)
    
    # <-- MODIFIED: Added sdu_can_fp=None -->
    def convert_csv_to_blf(self, pmz_can_fp, debug_can_fp, blf_fp, sdu_can_fp=None,
                           start_time_unix=None, stop_time_unix=None):
        """Convert CSV files to BLF format and optionally upload to GCS"""
        try:
            from ConvertCSV2BLF_ForTwoCANs import write_blf, upload_blf_to_gcs

            # <-- MODIFIED: Pass sdu_can_fp to write_blf -->
            success = write_blf(pmz_can_fp, debug_can_fp, blf_fp, log_callback=self.log_message, sdu_can_fp=sdu_can_fp)

            if success:
                self.log_message(f"BLF file created: {blf_fp}")

                # Auto-upload to GCS in a separate thread so conversion returns immediately
                if getattr(self, 'auto_upload_blf', False) and getattr(self, 'test_name', ''):
                    self.log_message("Queuing BLF upload to GCS (background)...")
                    upload_thread = threading.Thread(
                        target=self._upload_with_logging,
                        args=(blf_fp,),
                        kwargs={
                            'test_name': self.test_name,
                            'test_setup': getattr(self, 'test_setup', ''),
                            'sample_no': getattr(self, 'sample_no', ''),
                            'environment': getattr(self, 'ddg_environment', 'dev'),
                            'backend_api_url': getattr(self, 'backend_api_url', ''),
                            'battery_pack_id': self.batpack_id,
                            'start_time': start_time_unix,
                            'end_time': stop_time_unix,
                        },
                        daemon=False,
                    )
                    upload_thread.start()
                    self._pending_uploads.append(upload_thread)
            else:
                self.log_message(f"Error creating BLF file")

        except Exception as e:
            error_msg = f"Error converting CSV to BLF: {str(e)}"
            self.log_message(error_msg)
            import traceback
            traceback.print_exc()
    
    # <-- MODIFIED: Added sdu_can_fp=None -->
    def convert_csv_to_blf_with_gui_update(self, pmz_can_fp, debug_can_fp, blf_fp, sdu_can_fp=None):
        try:
            from ConvertCSV2BLF_ForTwoCANs import write_blf
            
            # Run the conversion without GUI callback
            # <-- MODIFIED: Pass sdu_can_fp to write_blf -->
            success = write_blf(pmz_can_fp, debug_can_fp, blf_fp, sdu_can_fp=sdu_can_fp)
            
            # Send a message back to the GUI thread
            if hasattr(self, 'log_callback') and self.log_callback:
                # Use the main thread's event loop if available
                if hasattr(self, 'main_thread_callback'):
                    # Queue the log message to be processed by the GUI thread
                    self.main_thread_callback(lambda: self.log_callback("Conversion Done!"))
                else:
                    # Directly call the callback (might cause thread issues)
                    self.log_callback("Conversion Done!")
                    
        except Exception as e:
            error_msg = f"Error converting CSV to BLF: {str(e)}"
            print(error_msg)


 
    
    def start_recording(self, batpack_id=None):
        """Start the bus recording process
        
        Args:
            batpack_id: Optional batpack ID to use for recording. If not provided,
                        uses the one stored in the class instance.
        """
        if getattr(self, '_is_running', False):
            self.log_message("Bus recording is already active. Ignoring duplicate start request.")
            return True

        self.log_message("Bus recording process starting...")
        self._is_running = True # Set the running flag
        
        # If batpack_id is provided, update the stored value
        if batpack_id is not None:
            self.set_batpack_id(batpack_id)
        
        # Initialize COM for this thread
        pythoncom.CoInitialize()
        
        try:
            # Get necessary variables and instruments
            variables = self.Application.ActiveExperiment.Platforms[0].ActiveVariableDescription.Variables
            recorder_state = self.Application.LayoutManagement.ActiveLayout.Instruments.Item("MultiState Display_Recorder")
            recorder_counter = self.Application.LayoutManagement.ActiveLayout.Instruments.Item("Display_RecorderCounter")
            self.timerecorded=self.Application.LayoutManagement.ActiveLayout.Instruments.Item("TimeRecorded")
            
            # Use the batpack_id from the GUI if we have it, otherwise get it from ControlDesk
            try:
                batID_control = self.Application.LayoutManagement.Layouts['PS_Ctrl'].Instruments.Item("BatPack_ID")
                
                # If we have a batpack_id from the GUI, update the control
                if self.batpack_id != "Unknown":
                    batID_control.Value = str(self.batpack_id)
                    self.log_message(f"Using BatPack ID from GUI: {self.batpack_id}")
                else:
                    # Otherwise get it from the control
                    self.batpack_id = str(batID_control.Value)
                    #self.log_message(f"Using BatPack ID from ControlDesk: {self.batpack_id}")
                    
            except Exception as e:
                self.log_message(f"Warning: Could not access BatPack_ID control: {str(e)}")
                # Continue with the batpack_id we have
            
            # Get CAN bus loggers
            Cd_log_Ch3 = self.Application.BusNavigator.GetSystems(1).Item(0).BusPlatforms[0].CANBusSystem.PhysicalBusAccesses.Item(0).Loggers.Item(0)
            Cd_log_Ch2 = self.Application.BusNavigator.GetSystems(1).Item(0).BusPlatforms[0].CANBusSystem.PhysicalBusAccesses.Item(1).Loggers.Item(0)
            Cd_log_Ch1 = self.Application.BusNavigator.GetSystems(1).Item(0).BusPlatforms[0].CANBusSystem.PhysicalBusAccesses.Item(2).Loggers.Item(0)

            
            # Initialize variables
            debug_on = int(self.Application.LayoutManagement.ActiveLayout.Instruments.Item("Radio Button_124547"))
            # pwr = 7  <- No longer needed
            counter_i = 0
            start_time = time.time()
            timestamp_start_fp = strftime("%Y-%m_%d %H_%M_%S", gmtime())
            
            self.log_message(f"Bus recording initialized at {timestamp_start_fp} for BatPack ID: {self.batpack_id}")
            
            # Main recording loop - controlled by self._is_running flag
            while self._is_running:
                # Update values
                #Platform()://Model Root/Simulator/PowerSupply_LV/PS_UMonitor_LV_PHY/Out
                Lv_volt = variables["Model Root/Simulator/PowerSupply_LV/PS_UMonitor_LV_PHY/Out"].ValueConverted
                debug_on = int(self.Application.LayoutManagement.ActiveLayout.Instruments.Item("Radio Button_124547"))
                
                time.sleep(1)
                counter_i = counter_i + 1
                recorder_counter.Value = counter_i
                self.time_diff = 0
                
                # Send update to GUI if callback is set
     
                if self.update_callback:
                    try:
                        # Get the LV voltage
                        Lv_volt = variables["Model Root/Simulator/PowerSupply_LV/PS_UMonitor_LV_PHY/Out"].ValueConverted
                        
                        Lv_current=variables["Model Root/Simulator/PowerSupply_LV/PS_IMonitor_LV_PHY/Out"].ValueConverted
                        
                        # Call callback with all three parameters
                        self.update_callback(counter_i, self.time_diff, Lv_volt, Lv_current)
                    except Exception as e:
                        self.log_message(f"Error in update callback: {str(e)}")
                
                # Reset counter if it reaches 10
                if counter_i == 10:
                    counter_i = 0
                
                # Check if logging is active and needs to be stopped

                if Cd_log_Ch3.State == 1:
                    Cd_log_Ch3.Stop()
                    Cd_log_Ch2.Stop()
                    if self.can_mode == "3-CAN":
                        Cd_log_Ch1.Stop()

                    stop_time_unix = time.time()  # Current UNIX timestamp

                    # Convert UNIX timestamp to UK time (BST/GMT as appropriate)
                    uk_timezone = pytz.timezone('Europe/London')
                    uk_stop_time = datetime.datetime.fromtimestamp(stop_time_unix, tz=uk_timezone)

                    # Format using the datetime object's strftime
                    timestamp_fp = uk_stop_time.strftime("%Y-%m_%d %H_%M_%S")
                    # Log the stop of recording with timestamp
                    self.log_message(f"{timestamp_fp}: stopped logging for BatPack ID: {self.batpack_id}")
                    
                    # Create new file paths with timestamps
                    debug_CAN_fp_new = f"N:\\TEST_DATA\\Dspace_CAN_Logging\\Debug_CAN\\{str(self.batpack_id)}Debug_CAN_ST{timestamp_start_fp}_SP{timestamp_fp}.csv"
                    pmz_CAN_fp_new = f"N:\\TEST_DATA\\Dspace_CAN_Logging\\PMZ_CAN\\{str(self.batpack_id)}PMZ_CAN_ST{timestamp_start_fp}_SP{timestamp_fp}.csv"
                    sdu_CAN_fp_new = f"N:\\TEST_DATA\\Dspace_CAN_Logging\\SDU_CAN\\{str(self.batpack_id)}SDU_CAN_ST{timestamp_start_fp}_SP{timestamp_fp}.csv"
                    blf_fp = f"N:\\TEST_DATA\\Dspace_CAN_Logging\\BLF_files\\{str(self.batpack_id)}PMZ_Debug_Merged_ST{timestamp_start_fp}_SP{timestamp_fp}.blf"
                    
                    # Rename files and update recorder state
                    os.rename(self.debug_CAN_fp, debug_CAN_fp_new)
                    os.rename(self.pmz_CAN_fp, pmz_CAN_fp_new)
                    if self.can_mode == "3-CAN":
                        os.rename(self.sdu_CAN_fp, sdu_CAN_fp_new)
                    recorder_state.Value = 0
                    

                    self.log_message(f"Starting BLF conversion for {os.path.basename(blf_fp)}")
                    # Create a reference to self.log_message for the thread
                    #log_callback = self.log_message

                    sdu_to_pass = sdu_CAN_fp_new if self.can_mode == "3-CAN" else None

                    # Convert CSV to BLF in a separate thread using threading module
                    conversion_thread = threading.Thread(
                        target=self.convert_csv_to_blf,
                        args=(pmz_CAN_fp_new, debug_CAN_fp_new, blf_fp, sdu_to_pass,
                              start_time, stop_time_unix),
                        daemon=False
                    )
                    conversion_thread.start()
                
                # Inner recording loop when voltage is above threshold and within time limit
                # Note: This inner loop structure seems complex. Consider refactoring 
                # if the logic can be simplified. Does it need to be nested?
                while self._is_running and self.time_diff/60 < self.fileMaxTime and Lv_volt>10:

                    counter_i = counter_i + 1
                    #self.log_message(f"Debug on2: over 12v {debug_on} {counter_i}")
                    recorder_counter.Value = counter_i
                    
                    current_time = time.time()
             
                    # Reset counter when it reaches 10
                    if counter_i == 10:
                        counter_i = 0
                    
                    # Update values
                    Lv_volt = variables["Model Root/Simulator/PowerSupply_LV/PS_UMonitor_LV_PHY/Out"].ValueConverted
                    debug_on = int(self.Application.LayoutManagement.ActiveLayout.Instruments.Item("Radio Button_124547"))
                    if Cd_log_Ch3.State == 0:
                        recorder_state.Value = 1
                        start_time = time.time()
                        uk_timezone = pytz.timezone('Europe/London')
                        uk_time = datetime.datetime.fromtimestamp(start_time, tz=uk_timezone)
                        # Format using the datetime object's strftime
                        timestamp_start_fp = uk_time.strftime("%Y-%m_%d %H_%M_%S")
            
                        Cd_log_Ch3.Start()
                        Cd_log_Ch2.Start()
                        if self.can_mode == "3-CAN": # <-- ADDED FOR 3-CAN MODE
                            Cd_log_Ch1.Start()
                        self.log_message(f"{timestamp_start_fp}: start logging")
                    
                    # Add a small delay
                    time.sleep(1)
                    self.time_diff = current_time - start_time
                    #self.log_message(f"Time diff: {self.time_diff}")
                    self.timerecorded.Value = self.time_diff/60
                # Send update to GUI if callback is set
                    if self.update_callback:             
                        try:
                            # Get the LV voltage
                            Lv_volt = variables["Model Root/Simulator/PowerSupply_LV/PS_UMonitor_LV_PHY/Out"].ValueConverted
                            Lv_current=variables["Model Root/Simulator/PowerSupply_LV/PS_IMonitor_LV_PHY/Out"].ValueConverted
                            # Call callback with all three parameters
                            self.update_callback(counter_i, self.time_diff, Lv_volt, Lv_current)
                        except Exception as e:
                            self.log_message(f"Error in update callback: {str(e)}")
            
            self.log_message(f"Bus recording loop finished for BatPack ID: {self.batpack_id}")
            return True
            
        except Exception as e:
            error_msg = f"Error in bus recording: {str(e)}"
            self.log_message(error_msg)
            import traceback
            traceback.print_exc()
            self._is_running = False # Ensure flag is false on error exit
            return False
        finally:
            # Uninitialize COM for this thread
            pythoncom.CoUninitialize()
            self.log_message("BusRecorder thread CoUninitialized.")

    def _upload_with_logging(self, blf_fp, **kwargs):
        """Wrapper around upload_blf_to_gcs that logs success/failure."""
        try:
            from ConvertCSV2BLF_ForTwoCANs import upload_blf_to_gcs
            success = upload_blf_to_gcs(
                blf_file_path=blf_fp,
                log_callback=self.log_message,
                **kwargs,
            )
            if success:
                self.log_message(f"GCS upload SUCCESS: {os.path.basename(blf_fp)}")
            else:
                self.log_message(f"GCS upload FAILED: {os.path.basename(blf_fp)}")
        except Exception as e:
            self.log_message(f"GCS upload ERROR: {e}")

    def wait_for_uploads(self, timeout=60):
        """Wait for any pending GCS uploads to finish. Called before app exit."""
        pending = [t for t in self._pending_uploads if t.is_alive()]
        if not pending:
            return
        self.log_message(f"Waiting for {len(pending)} pending upload(s) to finish...")
        for t in pending:
            t.join(timeout=timeout)
        still_alive = [t for t in pending if t.is_alive()]
        if still_alive:
            self.log_message(f"WARNING: {len(still_alive)} upload(s) did not finish in time.")
        else:
            self.log_message("All uploads completed.")
        self._pending_uploads = [t for t in self._pending_uploads if t.is_alive()]

    def stop(self):
        """Signals the recording loop to stop"""
        self.log_message("Stopping BusRecorder loop...")
        self._is_running = False


class dSpace_ControlDesk:
    """Class to handle interactions with dSpace ControlDesk application"""
    
    def __init__(self, log_callback=None):
        """Initialize ControlDesk interface
        
        Args:
            log_callback: Optional callback function for logging
        """
        self.ControlDeskApplication = None
        self.Enums = None
        self.bus_recorder = None
        self._pending_batpack_id = None  # Store ID if set before initialization
        self.log_callback = log_callback

    
    def log_message(self, message):
        """Log a message using the callback if available, otherwise print it"""
        if self.log_callback:
            self.log_callback(message)
        else:
            print(message)

    def set_file_max_time(self, minutes):
        """Set max recording time before CSV→BLF conversion (in minutes)."""
        if self.bus_recorder:
            self.bus_recorder.set_file_max_time(minutes)
        else:
            self.log_message(f"dSpace_ControlDesk: bus_recorder not ready, fileMaxTime {minutes} will be set on init")
            self._pending_file_max_time = minutes

    def set_can_mode(self, mode):
        """Set the CAN recording mode (2-CAN or 3-CAN)""" # <-- ADDED FOR 3-CAN MODE
        if self.bus_recorder:
            self.bus_recorder.can_mode = mode
            self.log_message(f"dSpace_ControlDesk: Mode updated to {mode}")
            
    def set_batpack_id(self, batpack_id):
        """Set the BatPack ID for use in bus recording
        
        Args:
            batpack_id (str): The BatPack ID to set
        """
        self.log_message(f"dSpace_ControlDesk: Setting BatPack ID to: {batpack_id}")
        
        # Store the ID for later if ControlDesk isn't initialized yet
        if not self.ControlDeskApplication or not self.bus_recorder:
            self._pending_batpack_id = batpack_id
            self.log_message(f"ControlDesk not fully initialized. BatPack ID {batpack_id} will be set when initialized.")
            return
        
        # Pass the ID to the bus recorder
        self.bus_recorder.set_batpack_id(batpack_id)
    
    def Initlize_close(self):   
        self.ControlDeskApplication = Dispatch("ControlDeskNG.Application")
    
    def Initialize(self, visible=False):
        pythoncom.CoInitialize()  # Initialize COM library
        self.ControlDeskApplication = Dispatch("ControlDeskNG.Application")
        self.Enums = Enums(self.ControlDeskApplication)
        self.ControlDeskApplication.MainWindow.Visible = visible
        """
        self.ControlDeskApplication.openExperiment(
            #r'C:\HiL\18_Bat_controldesk\01_BT_MLA_VB_DS_Mon\Instumentation\CLD_Prj_MLA_VB_v11\CLD_Prj_MLA_VB_v11.CDP', 'BTF_Stimulus', #Jerry local pc path test
            #r'C:\00_BTF_Projects\01_Scalexio_Projects\01_BT_MLA_VB_DS_Mon\Instumentation\CLD_Prj_MLA_VB_v11\CLD_Prj_MLA_VB_v11.CDP', 'BTF_Stimulus', #vib 1 MLA
            r'C:\00_BTF_Projects\01_Scalexio_Projects\03_BTF_DS_EMA_Prj\Instrumentation\CLD_Prj_EMA_IPB2_NCR24Q2_v24\CLD_Prj_EMA_IPB2_NCR24Q2_v24.CDP','BTF_Experiment' #vib 2 EMA
        )
        """
        # Initialize the bus recorder with the ControlDesk application
        self.bus_recorder = BusRecorder(self.ControlDeskApplication)
        
        # Pass any existing log callback to the bus recorder
        if self.log_callback:
            self.bus_recorder.set_log_callback(self.log_callback)
        
        # Set pending BatPack ID if any
        if self._pending_batpack_id:
            self.bus_recorder.set_batpack_id(self._pending_batpack_id)
            self._pending_batpack_id = None

        # Set pending fileMaxTime if any
        if getattr(self, '_pending_file_max_time', None):
            self.bus_recorder.set_file_max_time(self._pending_file_max_time)
            self._pending_file_max_time = None

    def ConnectAndStartOnlineCalibration(self):
        self.Initialize()
        Scalexio_platform = self.ControlDeskApplication.ActiveExperiment.Platforms[0]
        try:
            self.log_message("Connecting to platform...")
            Scalexio_platform.Connect()  # connect to the platform
            try:
                # Start online calibration
                self.log_message("Starting online calibration...")
                self.ControlDeskApplication.CalibrationManagement.StartOnlineCalibration()
            except Exception as ee:
                self.log_message(f"Error starting online calibration: {ee}")

        except Exception as e:
            self.log_message(f"Error connecting to platform: {e}")

    def set_vbatt_state(self, state):
        """Set the VBatt state in ControlDesk
        
        Args:
            state: Integer value (1 for ON, 0 for OFF)
        """
        if not self.ControlDeskApplication:
            print("ControlDesk application is not initialized")
            return False
        
        try:
            print(f"Setting VBatt state to: {state}")
            
            # Set the variable in ControlDesk
            variables = self.ControlDeskApplication.ActiveExperiment.Platforms[0].ActiveVariableDescription.Variables
            variables['Model Root/ControlPanel/PowerSupply/LC_PS_Btn_Ctrl/Value'].ValueConverted = state
            
            # Log success
            print(f"VBatt state set to: {state}")
            if self.log_callback:
                self.log_callback(f"VBatt state set to: {'ON' if state == 1 else 'OFF'}")
            
            return True
        except Exception as e:
            print(f"Error setting VBatt state: {str(e)}")
            if self.log_callback:
                self.log_callback(f"Error setting VBatt state: {str(e)}")
            import traceback
            traceback.print_exc()
            return False

    def get_lv_voltage(self):
        """Get the current LV Voltage from ControlDesk

        Returns:
            float or None: The voltage value, or None if unavailable or error.
        """
        if not self.ControlDeskApplication or not hasattr(self.ControlDeskApplication, 'ActiveExperiment'):
            self.log_message("ControlDesk not fully initialized or no active experiment.")
            return None
        
        try:
            # Ensure COM is initialized for this thread context if called directly
            # It might be safer to assume COM is initialized by the caller thread
            # pythoncom.CoInitialize() 
            
            variables = self.ControlDeskApplication.ActiveExperiment.Platforms[0].ActiveVariableDescription.Variables
            lv_voltage = variables["Model Root/Simulator/PowerSupply_LV/PS_UMonitor_LV_PHY/Out"].ValueConverted
            
            # pythoncom.CoUninitialize() # Balance if initialized here
            return float(lv_voltage)
        except pythoncom.com_error as com_err:
            self.log_message(f"COM Error getting LV voltage: {com_err}")
            # Attempt to uninitialize COM on error if it was initialized here
            # try: pythoncom.CoUninitialize() except: pass
            return None
        except KeyError:
            self.log_message("Error: LV Voltage variable path not found.")
            # try: pythoncom.CoUninitialize() except: pass
            return None
        except Exception as e:
            self.log_message(f"Error getting LV voltage: {str(e)}")
            # try: pythoncom.CoUninitialize() except: pass
            import traceback
            traceback.print_exc()
            return None

    def set_update_callback(self, callback_func):
        """Set a callback function for real-time updates
        
        Args:
            callback_func: Function that will be called with recorder_counter and time_recorded values
        """
        if not self.bus_recorder:
            self.bus_recorder = BusRecorder(self.ControlDeskApplication)
        self.bus_recorder.set_update_callback(callback_func)

    def set_window_visibility(self, visible):
        """Set the visibility of the ControlDesk main window
        
        Args:
            visible: Boolean indicating if the window should be visible
        """
        try:
            if self.ControlDeskApplication and hasattr(self.ControlDeskApplication, 'MainWindow'):
                self.ControlDeskApplication.MainWindow.Visible = visible
                return True
            return False
        except Exception as e:
            print(f"Error setting window visibility: {str(e)}")
            return False

    def Bus_record(self, batpack_id=None, update_callback=None):
        self.log_message("Starting Bus_record")
        
        # Make sure ControlDesk is initialized
        if not self.ControlDeskApplication:
            self.ConnectAndStartOnlineCalibration()
        
        # Initialize bus recorder if needed
        if not self.bus_recorder:
            self.bus_recorder = BusRecorder(self.ControlDeskApplication)
        
        # Set BatPack ID if provided
        if batpack_id is not None:
            self.bus_recorder.set_batpack_id(batpack_id)
        
        # Set update callback if provided
        if update_callback is not None:
            self.bus_recorder.set_update_callback(update_callback)
        
        # Pass any existing log callback to the bus recorder
        if self.log_callback and self.bus_recorder:
            self.bus_recorder.set_log_callback(self.log_callback)
        
        self.log_message("ControlDesk initialized, starting bus recording")
        
        try:
            # Start recording in a new thread
            recording_thread = threading.Thread(
                target=lambda: self.bus_recorder.start_recording(batpack_id)
            )
            #recording_thread.daemon = True
            recording_thread.start()
            
            # Wait for a short time to confirm recording has started
            time.sleep(2)
            
            self.log_message("Bus recording thread started")
            return True
            
        except Exception as e:
            self.log_message(f"Error starting bus recording: {str(e)}")
            import traceback
            traceback.print_exc()
            return False

    def Close(self):
        """Close the ControlDesk application"""
        self.log_message("Attempting to close ControlDesk application...")
        
        # Signal the BusRecorder thread to stop first
        if self.bus_recorder:
            self.bus_recorder.stop()
            self.log_message("Waiting for BusRecorder thread to finish...")
            time.sleep(2) # Give the thread a moment to exit its loop

        # Re-dispatch to ensure we have a valid COM object for Quit
        # Note: This might not be necessary if self.ControlDeskApplication is still valid
        # But can help if the object became stale.
        try:
            self.ControlDeskApplication = Dispatch("ControlDeskNG.Application")
        except Exception as dispatch_error:
             self.log_message(f"Could not re-dispatch ControlDesk Application: {dispatch_error}. Proceeding with existing object.")

        try:
            if self.ControlDeskApplication:
                try:
                    if hasattr(self.ControlDeskApplication, 'CalibrationManagement') and self.ControlDeskApplication.CalibrationManagement.IsOnlineCalibrationActive:
                        self.log_message("Stopping online calibration...")
                        self.ControlDeskApplication.CalibrationManagement.StopOnlineCalibration()
                    else:
                         self.log_message("Online calibration not active or not available.")
                except Exception as cal_e:
                    self.log_message(f"Error stopping online calibration: {str(cal_e)}")
                    # Continue shutdown even if calibration stop fails
                
                self.log_message("Calling Quit() method...")
                self.ControlDeskApplication.Quit()
                self.log_message("ControlDesk Quit() called successfully")
                self.ControlDeskApplication = None # Release reference
                self.bus_recorder = None # Release reference
                return True
            else:
                 self.log_message("ControlDeskApplication object is None, cannot Quit.")
                 return False
            
        except Exception as e:
            self.log_message(f"Error closing ControlDesk: {str(e)}")
            import traceback
            traceback.print_exc()
            
            # Clean up references even if there was an error
            self.ControlDeskApplication = None
            self.bus_recorder = None
            return False
    
    def close(self):
        """Alias for Close to provide case-insensitive access and handle COM init/uninit"""
        try:
            # Initialize COM in this thread if needed
            import pythoncom
            pythoncom.CoInitialize()
            
            result = self.Close() # Call the main Close logic
            
            # Uninitialize COM before returning
            pythoncom.CoUninitialize()
            return result
        except Exception as e:
            self.log_message(f"Error in close alias method: {str(e)}")
            import traceback
            traceback.print_exc()
            # Attempt to uninitialize COM even on error
            try:
                pythoncom.CoUninitialize()
            except Exception as uninit_e:
                 self.log_message(f"Error during CoUninitialize in close alias: {uninit_e}")
            return False

    def run(self):
        self.Bus_record()

    def startApp(self, cdp_file_path, experiment_name, visible=True):
        """Start ControlDesk and open the specified experiment
        
        Args:
            cdp_file_path: Path to the .CDP file to open
            experiment_name: Name of the experiment to load
            visible: Whether the ControlDesk main window should be visible
            
        Returns:
            bool: True if successful, False otherwise
        """
        # Initialize COM for this thread
        pythoncom.CoInitialize()
        success_flag = False
        try:
            # Create new instance of ControlDesk Application
            self.log_message("Creating ControlDesk Application instance...")
            self.ControlDeskApplication = Dispatch("ControlDeskNG.Application")
            
            # Set main window visibility based on parameter
            self.ControlDeskApplication.MainWindow.Visible = visible
            if not visible:
                self.log_message("ControlDesk window set to invisible mode")
            
            # Open the specified experiment
            self.log_message(f"Opening experiment: {experiment_name} from {cdp_file_path}")
            self.ControlDeskApplication.OpenExperiment(cdp_file_path, experiment_name)
            self.ControlDeskApplication.LayoutManagement.Layouts["PS_Ctrl"].Activate()

            #connect and start online calibration
            self.ConnectAndStartOnlineCalibration()
            
            # Initialize bus recorder
            self.bus_recorder = BusRecorder(self.ControlDeskApplication)
            
            # Set log callback for bus recorder
            if self.log_callback:
                self.bus_recorder.set_log_callback(self.log_callback)
                
            self.log_message("ControlDesk started successfully")
            success_flag = True
            
        except Exception as e:
            error_msg = f"Error starting ControlDesk: {str(e)}"
            self.log_message(error_msg)
            import traceback
            traceback.print_exc()
            success_flag = False
        finally:
             # Ensure COM is uninitialized for this thread
             pythoncom.CoUninitialize()
             self.log_message("startApp thread CoUninitialized.")
        return success_flag

def start():
    control_desk = dSpace_ControlDesk()
    timeout = 500  # 5 minutes in seconds

    # Create a thread for the run method
    control_desk_thread = threading.Thread(target=control_desk.run)
    control_desk_thread.start()
    control_desk_thread.join(timeout)

    if control_desk_thread.is_alive():
        print("Operation timed out")
        # Handle timeout (e.g., stop calibration, disconnect platform)
        control_desk_thread.join()
        if hasattr(control_desk, 'ControlDeskApplication'):
            if hasattr(control_desk.ControlDeskApplication, 'CalibrationManagement'):
                control_desk.ControlDeskApplication.CalibrationManagement.StopOnlineCalibration()
            if hasattr(control_desk.ControlDeskApplication, 'ActiveExperiment'):
                if hasattr(control_desk.ControlDeskApplication.ActiveExperiment, 'Platforms'):
                    control_desk.ControlDeskApplication.ActiveExperiment.Platforms[0].Disconnect()
    else:
        print("Operation completed successfully")





if __name__ == "__main__":
    start()
