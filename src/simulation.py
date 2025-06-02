import random
import heapq
from fvg_logging import CsvLogger as Logger
from fvg_logging import JsonLogger
from datetime import datetime
from abc import ABC, abstractmethod

# =============================================================================
# Events
# =============================================================================
class BaseEvent(ABC):
    """A base event is comprised of a timestamp and an associated simulation"""
    TYPE = None
    def __init__(self,timestamp,simulation):
        self._timestamp = timestamp
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

class Event(BaseEvent):
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
        super().__init__(timestamp=timestamp,simulation=simulation)
        self._car_id = car_id
        self._person_count = person_count
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
        if self._simulation.car_count() > 0:       # If there are still cars in the queue
            car = self._simulation.pop_car()
            event = TestingEvent(car[0],self._timestamp,car[1],self._simulation)
            self._simulation.add_event(event)
        else:                                       # If no cars need service
            self._simulation.free_server()
        self.log_self()

# =============================================================================
# Priority Queue
# =============================================================================
class PriorityQueue:
    """
    Baic priority queue. Wraps stored value into an Element object which is
     ordered according to the provided comparator function. If no comparator
     is provided a given value's built-in comparator is used instead
    Parameters:
        comparator: a comparator function meant to compare two provided elements. 
                    The user is responsible for making sure the comparator can 
                    compare stored objects
    """
    def __init__(self,comparator=None):
        if comparator is None:
            comparator = lambda x,y: -1 if x < y else 0 if x == 0 else 1
        self._comparator = comparator
        self._queue = []
        self._size = 0
        heapq.heapify(self._queue)  # Likely unnecessary
    def push(self,value):
        element = self.Element(value, self._comparator)
        heapq.heappush(self._queue,element)
        self._size += 1
    def pop(self):
        if self.is_empty(): return None
        element = heapq.heappop(self._queue)
        self._size -= 1
        return element.value
    def peek(self): # Expensive operation
        if self.is_empty(): return None
        value = self.pop()
        self.push(value)
        return value
    def is_empty(self):
        return self.size() == 0
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
# abstract class: BaseSimulation ----------------------------------------------
class BaseSimulation(ABC):
    """
    Abstract superclass for some functionalities.
    Every simulation should contain:
        - a method to initially populate the event queue
        - run the simulation
        - 
    """
    # core methods --------------------------------------------------
    def __init__(self):
        self._event_queue = PriorityQueue()
        self._populate_event_queue()
    def run(self):
        while not self._event_queue.is_empty():
            event = self._event_queue.pop()
            event.processEvent()
    @abstractmethod
    def _populate_event_queue(self):
        pass
    # event queue ---------------------------------------------------
    def add_event(self,event):
        self._event_queue.push(event)
    #def pop_event(self):
    #    return self._event_queue.pop()

# abstract class: BaseQueueServerSimulation -----------------------------------
class BaseQueueServerSimulation(BaseSimulation,ABC):
    """
    A Queue-Server simulation is a simulation that has events build around a set of servers
    servicing waiting objects in a queue
    """
    #TODO
    def __init__(self,queue_count,server_count,servicing_style,distribution_style):
        super().__init__()
        queues: list[PriorityQueue] = self._init_queues(queue_count,servicing_style)
        self._dist_manager: BaseDistributionManager = self._init_distribution_manager(server_count,distribution_style,queues)
    @abstractmethod
    def _init_queues(self, queue_count, servicing_style):
        pass
    @abstractmethod
    def _init_distribution_manager(self,server_count,distribution_style,queues):
        pass
    # distribution manager interaction ----------------------------------------
    def enqueue(self, item):
        self._dist_manager.enqueue(item)
    def update(self):
        self._dist_manager.update()


# class: SingleQueueMultiServerSimulation -------------------------------------
# [TODO] replace direct heapq for event queue by PriorityQueue
# [TODO] replace simple servicing style by server-car manager, as more complicated
#         interactions may be nessecary for task 2
class SingleQueueMultiServerSimulation(BaseSimulation):
    MAX_ARRIVAL_TIME = 7200 # Zeit in Sekunden
    MAX_SERVERS = 3

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

class CovidTestSimulation(BaseQueueServerSimulation):
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
    # LPT: The car with the most people has the longest processing time.
    def lpt_comp(car1,car2):
        if car1[1] > car2[1]: return -1
        elif car1[1] == car2[1]: return 0
        else: return 1
    def __init__(self,queue_count=3, server_count=3,servicing_style="fifo",distribution_style="oto"):
        super().__init__(queue_count=3, server_count=3,servicing_style="fifo",distribution_style="oto")
    
    def _init_queues(self, queue_count, servicing_style) -> list[PriorityQueue]:
        match(servicing_style):
            case "fifo": return [PriorityQueue(self.fifo_comp)]*queue_count
            case "lifo": return [PriorityQueue(self.lifo_comp)]*queue_count
            case "spt": return [PriorityQueue(self.spt_comp)]*queue_count
            case "lpt": return [PriorityQueue(self.lpt_comp)]*queue_count
            case _: return [PriorityQueue(self.fifo_comp)]*queue_count
    def _init_distribution_manager(self, server_count, distribution_style, queues):
        match(distribution_style):
            case "oto": self._dist_manager = OneToOneDistrubutionManager(server_count, self, queues=queues)
            case _: self._dist_manager = OneToOneDistrubutionManager(server_count, self)
    def _populate_event_queue(self):
        pass #TODO
        


# =============================================================================
# Distribution Manager
# =============================================================================
class BaseDistributionManager(ABC):
    """
    This class ist resposible for managing the distribution of cars from different
     queues to different servers.
    Remark: This class is basically a wrapper class for all queue and server functionalities
    [TODO] Different queue-distribution styles (Maybe different classes)
    """
    def __init__(self,server_count,simulation,queues=[]):
        """
        Instance Variables:
            _car_queues: list of PriorityQueues for car-tuples
            _servers: list of bool values. [k] == True <=> server k is available
        """
        self._queues: list[PriorityQueue] = queues
        self._servers: list[bool] = [True]*server_count
        self._simulation: SingleQueueMultiServerSimulation = simulation

    def enqueue(self, *args, **kwargs):
        pass

    @abstractmethod
    def update(self,timestamp):
        pass
        """[TODO] implement different servicing styles"""

class CovidSimDistributionManager(BaseDistributionManager,ABC):
    def enqueue(self, car: tuple):
        """
        This method adds new cars to the queue with the currently fewest cars.
         Only cars in the actual queues are considered; Cars currently served
         will be ignored.
        """
        current_queue = self._car_queues[0]
        cnt = current_queue.size()
        for queue in self._car_queues[1:]:
            val = queue.size()
            if val < cnt: 
                cnt = val
                current_queue = queue
        current_queue.push(car)
        
class OneToOneDistrubutionManager(CovidSimDistributionManager):
    """
    The user is resposible for making sure that there are at least
     as many queues as servers. If there are more queues than servers then all
     queues above the server count are ignored.
    """
    def update(self, timestamp):
        for i in range(len(self._servers)):
            if not self._servers[i]: continue # Do nothing if the server is busy
            car = self._car_queues[i].pop()
            if car is None: continue
            event = TestingEvent(car[0],timestamp,car[1],self._simulation)
            self._simulation.add_event(event)
            self._servers[i] = False

class LQFDistributionManager(BaseDistributionManager):
    """
    Largest Queue First Distribution Manager
    Remark: Single Queue Multi Server is supposed to be the same as LQFDM with
             only a single car_queue (TODO) 
    """
    # TODO
    def update(self, timestamp):
        pass
    