# Author: Tillmann Faust
# Python Version: Python 10.0.0

import random
import heapq
from fvg_logging import CsvLogger as Logger
from fvg_logging import JsonLogger
from datetime import datetime
from abc import ABC, abstractmethod

# =================================================================================================
# Priority Queue
# =================================================================================================
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
            comparator = lambda x,y: -1 if x < y else 0 if x == y else 1
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

# =================================================================================================
# Simulation
# =================================================================================================
# abstract class: BaseSimulation ----------------------------------------------
class BaseSimulation(ABC):
    """
    Abstract superclass for some functionalities.
    [TODO] logging
    """
    # core methods --------------------------------------------------
    def __init__(self):
        self._event_queue = PriorityQueue()
        self._populate_event_queue()
        self._logger: Logger = self._init_logger()
    def run(self):
        while not self._event_queue.is_empty():
            event: BaseEvent = self._event_queue.pop()
            event.processEvent()
    @abstractmethod
    def _populate_event_queue(self):
        pass
    # event queue ---------------------------------------------------
    def add_event(self,event):
        self._event_queue.push(event)
    # logging -------------------------------------------------------
    @abstractmethod
    def _init_logger(self):
        pass
    @abstractmethod
    def log(self, *args, **kwargs):
        pass

# abstract class: BaseQueueServerSimulation -----------------------------------
class BaseQueueServerSimulation(BaseSimulation,ABC):
    """
    A Queue-Server simulation is a simulation that has events built around a set of servers
    servicing waiting items in a queue
    """
    #TODO
    def __init__(self,queue_count,server_count,servicing_style,distribution_style):
        """
        Parameters:
            queue_count: Number of queues for items to be served
            server_count: Number of servers
            servicing_style: The way items are ordered in the item-queues
            distribution_style: The way queue-items are distributed to the servers
        """
        self._queue_count = queue_count
        self._server_count = server_count
        self._servicing_style = servicing_style
        self._distribution_style = distribution_style
        super().__init__()
        queues: list[PriorityQueue] = self._init_queues(queue_count,servicing_style)
        self._dist_manager: BaseDistributionManager = self._init_distribution_manager(server_count,distribution_style,queues)
    @abstractmethod
    def _init_queues(self, queue_count, servicing_style):
        pass
    @abstractmethod
    def _init_distribution_manager(self,server_count,distribution_style,queues):
        pass
    # distribution manager interaction ------------------------------
    def enqueue(self, item):
        self._dist_manager.enqueue(item)
    def update(self):
        self._dist_manager.update()
    def queued_item_count(self):
        return self._dist_manager.queued_item_count()
    def items_in_system(self):
        return self._dist_manager.items_in_system()

# class: CovidTestSimulation --------------------------------------------------
class CovidTestSimulation(BaseQueueServerSimulation):
    def __init__(
            self,
            queue_count=3, 
            server_count=3,
            servicing_style ="fifo",
            distribution_style="oto",
            max_arrival_time=7200,
        ):
        super().__init__(queue_count, server_count,servicing_style,distribution_style)
        self.MAX_ARRIVAL_TIME = max_arrival_time
    
    def _init_queues(self, queue_count, servicing_style) -> list[PriorityQueue]:
        match(servicing_style):
            case "fifo": return [PriorityQueue(self.fifo_comp) for _ in range(queue_count)]
            case "lifo": return [PriorityQueue(self.lifo_comp) for _ in range(queue_count)]
            case "spt": return [PriorityQueue(self.spt_comp) for _ in range(queue_count)]
            case "lpt": return [PriorityQueue(self.lpt_comp) for _ in range(queue_count)]
            case _: return [PriorityQueue(self.fifo_comp) for _ in range(queue_count)]
    def _init_distribution_manager(self, server_count, distribution_style, queues):
        match(distribution_style):
            case "oto": return OneToOneDistrubutionManager(server_count,self,queues=queues)
            case "lqf": return LQFDistributionManager(server_count,self,queues=queues)
            case _: return OneToOneDistrubutionManager(server_count,self,queues=queues)
    def _populate_event_queue(self):
        id_counter = 0
        time = random.randint(120, 180)
        while time <= self.MAX_ARRIVAL_TIME:
            event = ArrivalEvent(id_counter,time,random.randint(1,3),self)
            self.add_event(event)
            step = random.randint(120, 180)
            time += step
            id_counter += 1
    def _init_logger(self):
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        logname = f"simulation_log_{timestamp}"
        meta_logname = f"simulation_log_{timestamp}_meta"
        logger = Logger("time","car_id","person_count","event_type","cars_in_system",filename = logname, path = "./logs")
        meta_logger = JsonLogger(filename = meta_logname, path = "./logs")
        meta_logger.log("queue_count",str(self._queue_count))
        meta_logger.log("server_count",str(self._server_count))
        meta_logger.log("servicing_style",str(self._servicing_style))
        meta_logger.log("distribution_style",str(self._distribution_style))
        return logger
    def log(self,*args):
        self._logger.log(*args)
    def update(self,timestamp):
        self._dist_manager.update(timestamp)

    # car queue comparator functions ------------------------------------------
    # FIFO: The car with the lowest id has entered before every other car
    def fifo_comp(self,car1,car2):
        if car1[0] < car2[0]: return -1
        elif car1[0] == car2[0]: return 0
        else: return 1
    # LIFO: The car with the currently highest id has entered after every other
    def lifo_comp(self,car1,car2):
        if car1[0] > car2[0]: return -1
        elif car1[0] == car2[0]: return 0
        else: return 1
    # SPT: The car with the fewest people has the shortest processing time.
    def spt_comp(self,car1,car2):
        if car1[1] < car2[1]: return -1
        elif car1[1] == car2[1]: return 0
        else: return 1
    # LPT: The car with the most people has the longest processing time.
    def lpt_comp(self,car1,car2):
        if car1[1] > car2[1]: return -1
        elif car1[1] == car2[1]: return 0
        else: return 1
        


# =================================================================================================
# Distribution Manager
# =================================================================================================
class BaseDistributionManager(ABC):
    """
    This class ist resposible for managing the distribution of cars from different
     queues to different servers.
    Remark: This class is basically a wrapper class for all queue and server functionalities
    [TODO] Different queue-distribution styles (Maybe different classes)
    """
    def __init__(self,server_count,simulation,queues=[]):
        self._queues: list[PriorityQueue] = queues
        self._servers: list[Server] = [Server() for _ in range(server_count)]
        self._simulation: BaseQueueServerSimulation = simulation
    @abstractmethod
    def enqueue(self, *args, **kwargs):
        pass
    @abstractmethod
    def update(self,timestamp):
        pass
        """[TODO] implement different servicing styles"""
    def queued_item_count(self):
        return sum([queue.size() for queue in self._queues])
    def items_in_system(self):
        return (sum([queue.size() for queue in self._queues]) + sum([1 for val in self._servers if val.available == False]))

class CovidSimDistributionManager(BaseDistributionManager,ABC):
    def enqueue(self, car: tuple):
        """
        This method adds new cars to the queue with the currently fewest cars.
         Only cars in the actual queues are considered; Cars currently served
         will be ignored.
        """
        current_queue = self._queues[0]
        cnt = current_queue.size()
        for queue in self._queues[1:]:
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
            server = self._servers[i]
            # If a server is busy, check if the busy time is over and free if yes
            if not server.available:
                if server.busy_until > timestamp: continue
                else: server.free()
            # Fetch next car
            car = self._queues[i].pop()
            if car is None: continue
            # If a car could be fetched create the next events and lock the server until
            # the departure event.
            # I do not like this implementation, but i dont know how else to free the server
            # correctly yet.
            timestamp = timestamp + random.randint(60,120) + car[1] * 120
            testing_event = TestingEvent(car[0],timestamp,car[1],self._simulation)
            self._simulation.add_event(testing_event)
            server.lock(timestamp)

class LQFDistributionManager(BaseDistributionManager):
    """
    Largest Queue First Distribution Manager
    Remark: Single Queue Multi Server is supposed to be the same as LQFDM with
             only a single car_queue (TODO) 
    """
    # TODO
    def update(self, timestamp):
        for i in range(len(self._servers)):
            server = self._servers[i]
            # If a server is busy, check if the busy time is over and free if yes
            if not server.available:
                if server.busy_until > timestamp: continue
                else: server.free()
            current_queue = self._queues[0]
            cnt = current_queue.size()
            for queue in self._queues[1:]:
                val = queue.size()
                if val < cnt: 
                    cnt = val
                    current_queue = queue
            car = current_queue.pop()
            if car is None: break   # If the car is none, the largest queue has no cars => No cars can be serviced atm
            # If a car could be fetched create the next events and lock the server until
            # the departure event.
            # I do not like this implementation, but i dont know how else to free the server
            # correctly yet.
            timestamp = timestamp + random.randint(60,120) + car[1] * 120
            testing_event = TestingEvent(car[0],timestamp,car[1],self._simulation)
            self._simulation.add_event(testing_event)
            server.lock(timestamp)

# =================================================================================================
# Server
# =================================================================================================
class Server:
    """
    Simple wrapper class for server information
    """
    def __init__(self):
        self.available: bool = True
        self.busy_until: int = 0
    def lock(self, busy_until):
        self.available = False
        self.busy_until = busy_until
    def free(self):
        self.available = True

# =================================================================================================
# Events
# =================================================================================================
class BaseEvent(ABC):
    """A base event is comprised of a timestamp and an associated simulation"""
    TYPE = None
    def __init__(self,timestamp,simulation):
        self._timestamp = timestamp
        self._simulation: BaseQueueServerSimulation = simulation
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
    @abstractmethod
    def processEvent(self):
        pass

class CovidEvent(BaseEvent):
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
        self.done_at: int = timestamp
    def log_self(self):
        self._simulation.log(self._timestamp,self._car_id,self._person_count,self.TYPE,self._simulation.items_in_system())
    def processEvent(self):
        pass

# class: ArrivalEvent ---------------------------------------------------------
class ArrivalEvent (CovidEvent):
    """
    The ArrivalEvent initiates an event chain, by 
    """
    TYPE = "ARRIVAL"
    def processEvent(self):
        self._simulation.enqueue((self._car_id, self._person_count))
        self._simulation.update(self._timestamp)
        self.log_self()            

# class: PreregistrationEvent -------------------------------------------------
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
class TestingEvent (CovidEvent):
    """
    The TestingEvent calulates a timestamp 4 minutes per person in the future and creates
     a Departure event for that time
    """
    TYPE = "TESTING"
    def processEvent(self):
        event = DepartureEvent(self._car_id,self._timestamp,self._person_count,self._simulation)
        self._simulation.add_event(event)
        self.log_self()
        self._simulation.update(self._timestamp)

# class: DepartureEvent -------------------------------------------------------
class DepartureEvent (CovidEvent):
    """
    The last event in an event-chain. Removes the car from the simulator's car-queue
    """
    TYPE = "DEPARTURE"
    def processEvent(self):
        self._simulation.update(self._timestamp)
        self.log_self()