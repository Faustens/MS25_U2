from simulation import CovidTestSimulation as Simulation

# To emulate the first excercise slight adjustments to the code will have to be made
servicing_styles = ["fifo","lifo","spt","lpt"]
distributing_styles = ["oto","lqf"]

sim = Simulation(servicing_style="fifo",distribution_style="oto")
sim.run()