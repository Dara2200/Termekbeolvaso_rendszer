import time
import threading
import RPi.GPIO as GPIO

from tmc_driver.tmc_2209 import *
from tmc_driver._tmc_logger import Loglevel

# TMC2209 beállítása
tmc = Tmc2209(
    TmcEnableControlPin(26),
    TmcMotionControlStepDir(27, 17),
    TmcComUart("/dev/serial0"),
    loglevel=Loglevel.INFO
)

tmc.set_current(1600)
tmc.set_microstepping_resolution(2)
tmc.set_spreadcycle(False)
tmc.set_internal_rsense(False)
tmc.set_interpolation(True)
tmc.acceleration_fullstep = 1000
tmc.max_speed_fullstep = 250
tmc.set_motor_enabled(True)

# StallGuard beállítása
tmc._set_stallguard_threshold(85)
tmc._set_coolstep_threshold(100)

# GPIO beállítás a DIAG pin figyeléséhez
PIN_DIAG = 22
GPIO.setmode(GPIO.BCM)
GPIO.setup(PIN_DIAG, GPIO.IN)

# Változó, hogy jeleztünk-e már StallGuardot
stall_triggered = False

def poll_diag_pin():
    global stall_triggered
    while not stall_triggered:
        print(GPIO.input(PIN_DIAG))
        if GPIO.input(PIN_DIAG):
            print("StallGuard triggered!")
            tmc.tmc_mc.stop()
            stall_triggered = True
        time.sleep(0.01)

# Indítjuk a DIAG figyelő szálat
diag_thread = threading.Thread(target=poll_diag_pin)
diag_thread.daemon = True
diag_thread.start()

# Mozgatjuk a motort
print("Motor indul...")
result = tmc.run_to_position_steps(200, MovementAbsRel.RELATIVE)

if result is StopMode.NO:
    print("Mozgás sikeresen befejeződött.")
else:
    print("Mozgás megszakítva (pl. StallGuard).")

# Motor kikapcsolása
tmc.set_motor_enabled(False)
del tmc
GPIO.cleanup()

print("--- SCRIPT FINISHED ---")
