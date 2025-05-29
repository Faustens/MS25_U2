import random
import heapq
from fvg_logging import CsvLogger as Logger
from fvg_logging import JsonLogger
from datetime import datetime
from abc import ABC, abstractmethod

# =============================================================================
# Events
# =============================================================================
class Event:
    """
    Base event type. Provides compare functionality based on an events timestamp,
     a basic logging method and an empty processEvent-function 
    Parameters: 
        car_id: ID of the event's car
        timestamp: The simulated time in seconds when the event took place
        person_count: Amount of people in a given car
        simulation: The simulation object the event is to be associated with
    """
    TYPE = "BASE_EVENT"
    def __init__(self,car_id,timestamp,person_count,simulation):
        self._car_id = car_id
        self._timestamp = timestamp
        self._person_count = person_count
        self._simulation = simulation
    def __eq__(self,other):
        return self._timestamp == other._timestamp
    def __ne__(self,other):
        return self._timestamp != other._timestamp
    def __lt__(self,other):
        return self._timestamp < other._timestamp
    def __le__(self,other):
        return self._timestamp <= other._timestamp
    def __gt__(self,other):
        return self._timestamp > other._timestamp
    def __ge__(self,other):
        return self._timestamp >= other._timestamp
    def log_self(self):
        self._simulation.log(self._timestamp,self._car_id,self._person_count,self.TYPE,self._simulation.cars_in_system())
    def processEvent(self):
        pass

# class: ArrivalEvent ---------------------------------------------------------
class ArrivalEvent (Event):
    """
    The ArrivalEvent initiates an event chain, by 
    """
    TYPE = "ARRIVAL"
    def processEvent(self):
        if self._simulation.acqurie_server():   # Try to aquire a server; If available:
            event = TestingEvent(self._car_id,self._timestamp,self._person_count,self._simulation)
            self._simulation.add_event(event)
        else: # If no server is available enqueue car
            self._simulation.enqueue_car((self._car_id,self._person_count))
        self.log_self()            

# class: PreregistrationEvent -------------------------------------------------
# [TODO][Q] Is the PreregistrationEvent removed? Is there still a 1-2 minute waiting
#            time for preregistration? Does prereg happen before enqueueing the car?
#            idk.
'''
class PreregistrationEvent (Event):
    """
    A PreregistrationEvent happens immediately after an ArrivalEvent iff. the queue had room for
     the associated car. It determines a random time between 1-2 minutes and creates a
     TestingEvent at that timestamp
    """
    TYPE = "PREREG"
    def processEvent(self):
        timestamp = self._timestamp + random.randint(60,120)
        event = TestingEvent(self._car_id,timestamp,self._person_count,self._simulation)
        self._simulation.add_event(event)
        self.log_self()
'''

# class: TestingEvent ---------------------------------------------------------
class TestingEvent (Event):
    """
    The TestingEvent calulates a timestamp 4 minutes per person in the future and creates
     a Departure event for that time
    """
    TYPE = "TESTING"
    def processEvent(self):
        self.log_self()
        timestamp = self._timestamp + random.randint(60,120) + self._person_count * 120
        event = DepartureEvent(self._car_id,timestamp,self._person_count,self._simulation)
        self._simulation.add_event(event)

# class: DepartureEvent -------------------------------------------------------
class DepartureEvent (Event):
    """
    The last event in an event-chain. Removes the car from the simulator's car-queue
    """
    TYPE = "DEPARTURE"
    def processEvent(self):
        self.log_self()
        if self._simulation.car_count() > 0:       # If there are still cars in the queue
            car = self._simulation.pop_car()
            event = TestingEvent(car[0],self._timestamp,car[1],self._simulation)
            self._simulation.add_event(event)
        else:                                       # If no cars need service
            self._simulation.free_server()

# =============================================================================
# Priority Queue
# =============================================================================
class PriorityQueue:
    """
    Baic priority queue. Wraps stored value into an Element object which is
     ordered according to the provided comparator function
    Parameters:
        comparator: a comparator function meant to compare two provided elements. 
                    The user is responsible for making sure the comparator can 
                    compare stored objects 
    """
    def __init__(self,comparator):
        self._comparator = comparator
        self._queue = []
        self._size = 0
        heapq.heapify(self._queue)  # Likely unnecessary
    def push(self,value):
        element = self.Element(value, self._comparator)
        heapq.heappush(self._queue,element)
        self._size += 1
    def pop(self):
        element = heapq.heappop(self._queue)
        self._size -= 1
        return element.value
    def peek(self): # Expensive operation
        value = self.pop()
        self.push(value)
        return value
    def is_empty(self):
        return len(self._queue) == 0
    def size(self):
        return self._size

    class Element:
        def __init__(self,value,comparator):
            self._comparator = comparator
            self.value = value
        def __lt__(self,other):
            return self._comparator(self.value,other.value) < 0
        def __eq__(self,other):
            return self._comparator(self.value,other.value) == 0

# =============================================================================
# Simulation
# =============================================================================
# class: SingleQueueMultiServerSimulation -------------------------------------
# [TODO] replace direct heapq for event queue by PriorityQueue
# [TODO] replace simple servicing style by server-car manager, as more complicated
#         interactions may be nessecary for task 2
class SingleQueueMultiServerSimulation:
    MAX_ARRIVAL_TIME = 7200 # Zeit in Sekunden
    MAX_SERVERS = 3

    # car queue comparator functions ------------------------------------------
    # FIFO: The car with the lowest id has entered before every other car
    def fifo_comp(car1,car2):
        if car1[0] < car2[0]: return -1
        elif car1[0] == car2[0]: return 0
        else: return 1
    # LIFO: The car with the currently highest id has entered after every other
    def lifo_comp(car1,car2):
        if car1[0] > car2[0]: return -1
        elif car1[0] == car2[0]: return 0
        else: return 1
    # SPT: The car with the fewest people has the shortest processing time.
    def spt_comp(car1,car2):
        if car1[1] < car2[1]: return -1
        elif car1[1] == car2[1]: return 0
        else: return 1
    # LPT: The car with the fewest people has the shortest processing time.
    def lpt_comp(car1,car2):
        if car1[1] > car2[1]: return -1
        elif car1[1] == car2[1]: return 0
        else: return 1
    # Initializer -------------------------------------------------------------
    def __init__(self,servicing_style="fifo"):
        print("Init")
        self._event_queue = []
        heapq.heapify(self._event_queue)
        self._car_queue = None
        match(servicing_style):
            case "fifo": self._car_queue = PriorityQueue(self.fifo_comp)
            case "lifo": self._car_queue = PriorityQueue(self.lifo_comp)
            case "spt": self._car_queue = PriorityQueue(self.spt_comp)
            case "lpt": self._car_queue = PriorityQueue(self.lpt_comp)
            case _: raise Exception
        self._id_counter = 0
        self._available_servers = self.MAX_SERVERS
        self.populate_event_queue()
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        logname = f"simulation_log_{timestamp}"
        meta_logname = f"simulation_log_{timestamp}_meta"
        self._logger = Logger("time","car_id","person_count","event_type","cars_in_system",filename = logname, path = "./logs")
        self._meta_logger = JsonLogger(filename = meta_logname, path = "./logs")
        self._meta_logger.log("server_count",str(self.MAX_SERVERS))
        self._meta_logger.log("time_limit",str(self.MAX_ARRIVAL_TIME))
    
    def populate_event_queue(self):
        time = random.randint(120, 180)
        while time <= self.MAX_ARRIVAL_TIME:
            event = ArrivalEvent(self._id_counter,time,random.randint(1,3),self)
            self.add_event(event)
            step = random.randint(120, 180)
            time += step
            self._id_counter += 1
    # Event management ----------------------------------------------
    def add_event(self, event):
        heapq.heappush(self._event_queue,event)
    def pop_event(self):
        if not self._event_queue: return None
        return heapq.heappop(self._event_queue)
    # Car management ------------------------------------------------
    def enqueue_car(self, car: tuple):
        self._car_queue.push(car)
    def pop_car(self):
        return self._car_queue.pop()
    def car_count(self):
        """
        Returns the total amount of cars in the system, i.e. queued,
         as well as currently processed
        """
        return self._car_queue.size()
    def cars_in_system(self):
        return self._car_queue.size() + self.MAX_SERVERS - self._available_servers
    # Server management ---------------------------------------------
    def server_available(self):
        return self._available_servers > 0
    def acqurie_server(self):
        if not self.server_available(): return False
        self._available_servers -= 1
        return True
    def free_server(self):
        self._available_servers += 1
    # Run-method ----------------------------------------------------
    def run(self):
        """
        Iterates over the event-queue, calling each Event's processEvent-method
        """
        while(self._event_queue):
            event = self.pop_event()
            event.processEvent()
    # Logging interface ---------------------------------------------
    def log(self,*args) :
        """Logging interface for Event-Instances to use"""
        self._logger.log(*args)
