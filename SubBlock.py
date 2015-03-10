import numpy as np
import ephem
from matplotlib.patches import Rectangle
from matplotlib import dates
import matplotlib.dates as md
import math
from datetime import datetime, timedelta
import re

#Class definition
class SubBlock:
   #Constructor
   def __init__(self,ra,dec,blockid,WindowStart,WindowEnd,obstime,piname,propcode,targetname,priority,maxseeing,moonid,maxlunar,transparency,minlunardist,pirank,ignoretcmoon):
      self.ra = ra
      self.dec = dec
      self.blockid = blockid
      #URL to access block in web manager
      self.url = "https://www.salt.ac.za/wm/block/%d" % (self.blockid)
      #These values for track windows are stored in LST (as in DB)
      #self.dW1 and self.dW2 calculated later are in SAST and calculated post-constructor by CalcTrackTimes() - see below
      #The WindowStart and WindowEnd values are determined in the Queue class based on whether 
      #it's a continuous visibility window (e.g. Magellanic) or both east and west tracks (e.g. Galactic Bulge)
      self.W1 = WindowStart
      self.W2 = WindowEnd
      #using int() here to get around some weird bug: 
      #TypeError: unsupported type for timedelta seconds component: numpy.int16
      self.obstime = int(obstime) 

      #set to RSS as default instrument
      self.inst = "RSS"
      self.IsMOS = 0
      self.MosMaskLoaded = 0

      #assume block is not time critical by default
      self.istimecritical = 0
      self.timewindowactive = 0
      #acqtime is not currently used in the code, but it may be used to fine tune things more in the future 
      self.acqtime = 600
      #self.offset holds the randomly generated offset from the nominal start time of the block
      self.offset = 0
      #Assume OK to start with. Easier to prove not feasible by setting this to False than the other way around
      self.MoonOK = True
      self.TwilightOK = True 
      #not currently used, but may be used in future to pre-calculate and keep track of overlapping blocks 
      self.noverlaps = 0
      self.overlaps = []

      #Ignore the moon if the block is time critical
      self.ignoretcmoon = ignoretcmoon

      #These are used in working out the time windows 
      self.time1 = 0
      self.time2 = 0
      
      #Keeps track of whether the block is active in the queue
      self.isactive = 0
      #Is the block a target of opportunity (ToO) 
      self.istoo =0
      #Store visit information. This is simple at the moment but will need to be expanded later
      self.nvisits = 0
      self.ndone = 0
      self.waitdays = 0
 
      #A flag to keep track of whether block point window information is available
      #Non-sidereal blocks will not have block point windows
      #This is to avoid them crashing the whole scheduler page
      self.bpw = 0

      #Block specific information
      
      #PI Name
      self.piname = piname
      #Proposal Code
      self.propcode = propcode
      #Target Name
      self.targetname = targetname
      #PI rank (not used at present except in HTML display)
      self.priority = priority
      if(pirank == 1):
         self.pirank = "Hi"
      if(pirank == 2):
         self.pirank = "Med"
      if(pirank == 3):
         self.pirank = "Lo"
      #Cloud transparency
      self.transparency = "Any"
      if(transparency == 2):
         self.transparency = "Clear"
      if(transparency == 4):
         self.transparency = "Thick"
      if(transparency == 5):
         self.transparency = "Thin"

      #Priority-based colour scheme
      self.colour = 'cyan' #P4 
      if(self.priority == 0):
         self.colour='black'
      elif (self.priority == 1):
         self.colour='red'
      elif (self.priority == 2):
         self.colour='green'
      elif (self.priority == 3):
         self.colour='blue'

      #Max Seeing
      self.maxseeing = maxseeing

      #If minimum distance from moon is specified for the queue, use that
      #Otherwise, use 30 degrees as a default
      self.minlunardist = minlunardist
      if(minlunardist == 0.0):
         self.minlunardist = 30.0

      #Define moon lunar illum and color 
      #Note the checks for 0.0 are because lunar values are still not uniformly populated in the database by PIPT.
      self.minlunar = 0.0
      self.maxlunar = 100.0
      self.mooncolour='white'
      if(moonid == 3):
         self.minlunar = 0.0
         self.maxlunar = 15.0
         self.mooncolour='black'
         if(maxlunar != 'NULL' and maxlunar != 0.0 and maxlunar >= self.minlunar):
            self.maxlunar=maxlunar
      if(moonid == 5 or (moonid==2 or moonid==4)):
         self.minlunar = 15.0
         self.maxlunar = 85.0
         self.mooncolour='gray'
         if(maxlunar != 'NULL' and maxlunar != 0.0 and maxlunar >= self.minlunar):
            self.maxlunar=maxlunar
      if(moonid == 1):
         self.minlunar = 85.0
         self.maxlunar = 100.0
         self.mooncolour='yellow'
         if(maxlunar != 'NULL' and maxlunar != 0.0 and maxlunar >= self.minlunar):
            self.maxlunar=maxlunar

   def IsSlitmaskLoaded(self):
      #checks if the required slitmasks are available - mostly for MOS, but we could also check slitmask for longslits...
      #assume the longslits are all in
      if(self.IsMOS == 0):
         return 1
      else:
         return (self.IsMOS == 1 and self.MosMaskLoaded == 1)
   def GetTargetName(self):
      return self.targetname
   def GetMoonType(self):
      return self.mooncolour
   def GetTransparency(self):
      return self.transparency
   def GetPri(self):   
      return self.priority
   def GetPI(self):
      return self.piname
   def GetPIRank(self):
      return self.pirank
   def GetObsTime(self):
      return self.obstime
   def MaxSeeing(self):
      return self.maxseeing
   #convert coordinates to decimal degrees
   def deg2hms(self,n):
      d = int(n)
      m = int((n-d)*60)
      s = int((int((n-d)*60)-m)*60)
      return (d,m,s)
   def GetCoords(self):
      return (self.ra,self.dec)
   #We have to keep track of the other subblocks with the same blockid
   #so let's store a list of references
   #This can be done after constructor, since we read in info for each blockid separately and can create the separate subblocks
   #all at once as necessary
   def TrackOverlaps(self,ti,tf):
     #Not sure if the following N.B. is still valid (?)
     #N.B. we have to have self.dW2+self.obstime here, since self.dW2 is now modified to be the bpw.
     return (self.dW2 >= ti and self.dW1 <= tf)#(max(self.dW1,ti) <= min(self.dW2+timedelta(seconds=self.obstime),tf))

   def SetActive(self,flag):
      self.isactive=flag
   def SetDefaultStart(self):
      self.offset = 0
      self.b1=self.MinStartTime
      self.b2=self.b1+timedelta(seconds=self.obstime) 
   def Randomise(self):
      #print "Randomise"
      #Shuffle starting time of a block (within the track window)
      #making sure it could still be completed within the track window
      #post-conditions: return 0 if random step meant the block couldn't be completed at the newly proposed time
      #                 return 1 if it was a success and the block is now changed
      #(for now, it always returns 1, until we have more detailed track info/want to do more detailed checks)
      #np.random.random_sample()
      #backup old offset if unsuccessful so we can revert to previous state....
      #self.offset=int(np.random.random_sample()
      #prev_offset =self.offset
      #if(self.dW1 != self.dW2):
      self.offset = int(np.random.random_sample()*self.DurationSeconds)
      #print "offset = %d, duration %s" % (self.offset,self.DurationSeconds)
      self.b1=self.dW1+timedelta(seconds=self.offset)
      self.b2=self.b1+timedelta(seconds=self.obstime) 
      return 1
   def IsActive(self):
      return self.isactive
   #Should be expanded to support instrument mode descriptors separate to instrument 
   def InstrumentInfo(self):
      return self.inst
   #Query science database to find out what instrument a block is...it really is too complex to find out what instrument a block uses in the database. Simplifying this could reduce the load time substantially...
   def RetrieveInstrument(self,con,loadedmasks,loadedfilters):
      cur = con.cursor()
      qtxt = "select SalticamPattern_Id, RssPattern_Id, BvitPattern_Id, HrsPattern_Id from Block join Pointing using (Block_Id) join TelescopeConfigObsConfig using (Pointing_Id) join ObsConfig on (PlannedObsConfig_Id=ObsConfig_Id) join PayloadConfig using (PayloadConfig_Id) where PayloadConfigType_Id=3 and Block_Id=\'%s\'" % (self.blockid)
      cur.execute(qtxt)
      results = cur.fetchall()
      nrows = cur.rowcount

      SCAM = 0
      RSS = 0
      HRS = 0
      BVIT= 0
      propcode=""
      if(nrows > 0):
         for data in results:
            #print "SCAM=%s RSS=%s BVIT=%s HRS=%s" % (data[0],data[1],data[2],data[3])
            if(not SCAM):
               SCAM = (str(data[0]) != "None")
            if(not RSS):
               RSS  = (str(data[1]) != "None")
            if(not BVIT):
               BVIT = (str(data[2]) != "None")
            if(not HRS):
               HRS  = (str(data[3]) != "None")
            #print "SCAM=%s RSS=%s BVIT=%s HRS=%s" % (SCAM,RSS,BVIT,HRS)
         if(SCAM):
            self.inst = "SCAM"
         if(RSS):
            self.inst = "RSS"
            #find out what mode it is....
            #,ProposalCode.Proposal_Code
            qtxt = "Select Mode,Grating,rm.Barcode,rf.Barcode from Proposal join ProposalCode using (ProposalCode_Id) join Block using (Proposal_Id) join Pointing using (Block_Id) join TelescopeConfigObsConfig using (Pointing_Id) join ObsConfig on (PlannedObsConfig_Id=ObsConfig_Id) join PayloadConfig using (PayloadConfig_Id) join RssPatternDetail using (RssPattern_Id) join Rss using (Rss_Id) join RssConfig using (RssConfig_Id) Join RssMode using (RssMode_Id) left join RssSpectroscopy using (RssSpectroscopy_Id) left join RssGrating using (RssGrating_Id) left join RssMask as rm using (RssMask_Id) left join RssFilter as rf using (RSSFilter_Id) where Block.Block_Id=\'%s\'" % (self.blockid)
            cur.execute(qtxt)
            results2 = cur.fetchall()
            self.IsMOS = 0
            self.MosMaskLoaded = 0
            self.RssFilterLoaded = 1
            #this makes sure that all filters are available...i.e. that if we find a mismatch, set filter loaded == 0 and get out of loop
            for data2 in results2:
               if(not loadedfilters.has_key(str(data2[3]))):
                  self.RssFilterLoaded = 0
                  break

            for data2 in results2:
               #propcode=data2[3]
               if(str(data2[0]) == "MOS"):
                  self.IsMOS = 1
                  #The mask is currently loaded in RSS
                  if(loadedmasks.has_key(str(data2[2]))):
                     self.MosMaskLoaded = 1
                  break
         if(HRS):
            self.inst = "HRS"
         if(BVIT):
            self.inst = "BVIT"
         
         #print "%s self.blockid = %s, self.inst = %s, IsMOS=%s, MosMaskLoaded=%s" % (propcode,self.blockid,self.inst,self.IsMOS,self.MosMaskLoaded)
      if(self.IsMOS):
         self.acqtime = 900
      cur.close()
   #An example with MOS
#mysql> Select Mode,Grating,Barcode from Proposal join ProposalCode using (ProposalCode_Id) join Block using (Proposal_Id) join Pointing using (Block_Id) join TelescopeConfigObsConfig using (Pointing_Id) join ObsConfig on (PlannedObsConfig_Id=ObsConfig_Id) join PayloadConfig using (PayloadConfig_Id) join RssPatternDetail using (RssPattern_Id) join Rss using (Rss_Id) join RssConfig using (RssConfig_Id) Join RssMode using (RssMode_Id) left join RssSpectroscopy using (RssSpectroscopy_Id) left join RssGrating using (RssGrating_Id) left join RssMask using (RssMask_Id) where Block.Block_Id='25825';
#+---------+---------+------------+
#| Mode    | Grating | Barcode    |
#+---------+---------+------------+
#| MOS     | pg0900  | P001140N04 |
#| MOS     | pg0900  | P001140N04 |
#| Imaging | NULL    | P001140N04 |
#| Imaging | NULL    | NULL       |
#+---------+---------+------------+
   
   def RetrieveTimeWindows(self,con):
      #CRITICAL ASSUMPTION/FLAW: assumes only one time-critical window is available per sub-block
      # This may fall apart in long equatorial tracks that could potentially have two time-critical zones
      #The current code would not fall apart, it just may not pick up the other windows - since if the first one
      #is encountered that overlaps with the strict time window, this window is shrunk further. So unless a subsequent
      #time window overlaps another, it won't be considered. Nevertheless, we use a 'break' statement to stop the loop early through
      #all the other time windows from other nights

      #first check if there are time-critical windows available...
      cur = con.cursor()
      qtxt = "select ObsWindowStart,ObsWindowEnd from Block join Pointing using (Block_Id) join TimeRestricted using (Pointing_Id) where Block_Id=\'%s\' ORDER BY ObsWindowStart" % (self.blockid)
      cur.execute(qtxt)
      results = cur.fetchall()
      nrows = cur.rowcount
      #the time windows are formatted in the database as:
      #2014-07-26 18:07:53 ... 2014-07-26 18:50:28
      #with the times all being UT
      if(nrows > 0):
         self.istimecritical = 1
         for data in results:
            t1 = ephem.localtime(ephem.date(str(data[0])))
            t2 = ephem.localtime(ephem.date(str(data[1])))
            #use the strict times for the block...
            blockstart=self.MinStartTime
            blockend=self.MaxEndTime
            #if it overlaps with the time window, shrink the window at both ends (or only one end if possible)
            #(if and only if the window does not go outside the strict time window)
            #this is a simple solution - restricting the plausible times the block can be visited
            #to those of the time critical window, rather than trying to fit the block at the very start of
            #the window, which may not be possible to do (although, this may be desirable to 'enhance' the chances
            #of occurring in the optimisation of the queue
            if(self.IsOverlapping(t1,t2,blockstart,blockend)):
               #print "(orig)Min/Max: %s/%s Time window: %s %s\n" % (blockstart,blockend,t1,t2)
               self.timewindowactive=1
               if(t1-self.MinStartTime >= timedelta(seconds=0)):
                  self.MinStartTime = t1 
               if(self.MaxEndTime-t2 >= timedelta(seconds=0)):
                  self.MaxEndTime = t2
               #print "before: %s %s after: %s %s\n" % (blockstart,blockend,self.MinStartTime,self.MaxEndTime)
               #change the default start times to make sure we are within the (NEW) actual time window!!
               #very important
               if(self.MinStartTime-self.b1 >= timedelta(seconds=0)):
                  self.b1=self.MinStartTime
                  self.b2=self.b1+timedelta(seconds=self.obstime) 
               #and update the latest point times for Randomise()
                  self.LatestPointTime=self.MaxEndTime-timedelta(seconds=self.obstime)
                  self.LatestPointDuration=md.date2num(self.LatestPointTime)-md.date2num(self.MinStartTime)
                  self.LatestPointDurationSeconds = (self.LatestPointTime-self.MinStartTime).total_seconds()
                  self.WindowDuration = md.date2num(self.MaxEndTime)-md.date2num(self.MinStartTime)
               break
      cur.close()
  #def Print(self):
      #print some basic info
      #print "%s %s P%s %s" % (self.GetPropCode(),self.GetID(),self.GetPri(),self.GetPI())
   def IsTimeCritical(self):
      return self.istimecritical
   #True if it is either not time critical (ordinary blocks) OR if it is time critical and the window is active
   def IsTimeCriticalOK(self):
      return (self.istimecritical == False or (self.istimecritical and self.timewindowactive))
   def IsTimeWindowActive(self):
      #returns true if the time window of the block is 'live' for the night that the block is associated with
      return self.timewindowactive
   def GetID(self):
      return self.blockid
   #These overlap functions are not currently used...
   def AddTWOverlaps(self,overlaps):
      self.overlaps = overlaps
      self.noverlaps = len(overlaps)
   def NTWOverlaps(self):
      return self.noverlaps
   def TWOverlaps(self):
      return self.overlaps
   #This is more for debugging 
   def GetTimeWindows(self):
      return (self.time1,self.time2)
   def Energy(self):
      return self.energy
   #Called from the Queue class...basic for now
   #Note that it may depend on changing values during the night (seeing , etc) , so may not be a static value as currently used.
   #I think that's why I made this a separate function, so it can be called from the Queue class in case Queue conditions change
   def CalcEnergy(self):
      exp = 0.0
      base = 2.0
      if(self.priority == 0):
         exp = 10.0
      elif(self.priority == 1):
         exp = 8.0
      elif(self.priority == 2):
         exp = 7.0
      elif(self.priority == 3):
         exp = 6.0
      elif(self.priority == 4):
         exp = 3.0
      if(self.istimecritical):
         exp = exp + 1.0
      self.energy = math.pow(base,exp)
   def GetPropCode(self):   
      return self.propcode
   def GetMoonMinMax(self):
      return (self.minlunar,self.maxlunar)
   #Wrapping function for times
   def wrap(self,w):
      if(w >= 24):
         w -= 24.0
      elif(w < 0):
         w += 24.0
      return w
   #return moon distance
   def GetMoonDist(self):
      return self.moondist
   def IsTwilightOK(self):
      return self.TwilightOK
   def IsMoonOK(self,minmoondist):
      #return self.MoonOK modulo min lunar distance
      if(minmoondist > self.minlunardist):
         return (self.MoonOK and self.moondist >= minmoondist)
      else:
         return (self.MoonOK and self.moondist >= self.minlunardist)
   #def IsMoonDistOK(self):
   #   return (self.moondist >= self.minlunardist)
   def IsBlockOverlapping(self,Bstart,Bend):
      return ( max(self.b1,Bstart) <= min(self.b2,Bend))
   def IsOverlapping(self,Astart,Aend,Bstart,Bend):
      return ( max(Astart,Bstart) <= min(Aend,Bend))
   def SetColour(self,colour):
      self.colour=colour
   def GetColour(self):
      return self.colour
   #return url to access in WM
   def GetWMURL(self):   
      return self.url
   #The track LST times are fixed in science database, so we need to convert them to SAST 
   #The E1,E2,W1,W2 times are in LST (const)
   def CalcTrackTimes(self,LST_MIN_SAST,inputdate,con,illum,mstart,mend,ti,tf):
      #let's get the BlockPointWindows first - essentially these allow us to set harder limits on when it's safe to point to an object, 
      #Dates are in format YY/MM/DD
      #e.g. 2014/11/08
      #convert W1, W2 (in decimal LST) to dW1, dW2 in datetime format

      time1 = self.wrap(float(self.W1)-LST_MIN_SAST)
      (th,tm,ts) = self.deg2hms(time1)
      d1 = inputdate + " %d:%d:%d" % (th,tm,ts)
      date1 = datetime.strptime(d1,"%Y/%m/%d %H:%M:%S")
      if(time1 < 12):
         date1 += timedelta(hours=24)
      self.dW1 = date1

      time2 = self.wrap(float(self.W2)-LST_MIN_SAST)
      (th,tm,ts) = self.deg2hms(time2)
      d2 = inputdate + " %d:%d:%d" % (th,tm,ts)
      date2 = datetime.strptime(d2,"%Y/%m/%d %H:%M:%S")
      if(time2 < 12):
         date2 += timedelta(hours=24)
      self.dW2 = date2

      self.Duration = md.date2num(self.dW2)-md.date2num(self.dW1)
      self.dO1 = self.dW1
      self.dO2 = self.dW2
      self.DurationO = md.date2num(self.dO2)-md.date2num(self.dO1)

      #Block point windows assure that there's enough track time to complete the science observations if
      #the block is pointed to within the window specified
      cur = con.cursor()
      qtxt = "select bpw.PointingStart,bpw.PointingEnd from Block join BlockPointWindow as bpw using (Block_Id) where Block_Id=\'%s\'" % (self.blockid)
      cur.execute(qtxt)
      results = cur.fetchall()
      nrows = cur.rowcount
      self.bpw1 = None 
      self.bpw2 = None
      if(nrows > 0):
         self.bpw = 1
         for data in results:
            time1 = self.wrap(float(data[0])-LST_MIN_SAST)
            (th,tm,ts) = self.deg2hms(time1)
            d1 = inputdate + " %d:%d:%d" % (th,tm,ts)
            pstart = datetime.strptime(d1,"%Y/%m/%d %H:%M:%S")
            if(time1 < 12):
               pstart += timedelta(hours=24)
            
            time2 = self.wrap(float(data[1])-LST_MIN_SAST)
            (th,tm,ts) = self.deg2hms(time2)
            d2 = inputdate + " %d:%d:%d" % (th,tm,ts)
            pend = datetime.strptime(d2,"%Y/%m/%d %H:%M:%S")
            if(time2 < 12):
               pend += timedelta(hours=24)

           #So, the BPW cannot be extended outside the BPW
           #i.e. any changes based on twilight times, can only be trimmed within the valid BPW (provided the obstime can still be observed) 
           #therefore we don't have to explicity check that obstime is still available, we're still in a valid block point window
            if(max(pstart,self.dW1) <= min(pend,self.dW2)):
               self.dW1 = max(pstart,self.dW1)
               self.dW2 = min(pend,self.dW2)
               self.bpw1 = self.dW1
               self.bpw2 = self.dW2
               break

      cur.close()
      
      #T I M E   C R I T I C A L

      cur = con.cursor()
      qtxt = "select ObsWindowStart,ObsWindowEnd from Block join Pointing using (Block_Id) join TimeRestricted using (Pointing_Id) where Block_Id=\'%s\' ORDER BY ObsWindowStart" % (self.blockid)
      cur.execute(qtxt)
      results = cur.fetchall()
      nrows = cur.rowcount
      #the time windows are formatted in the database as e.g.:
      #2014-07-26 18:07:53 ... 2014-07-26 18:50:28
      #with the times all being UT
      self.timewindowactive=False
      if(nrows > 0):
         #we could alternatively set this below, only if the window overlaps
         #but the flag is defined in a more global sense (no matter when the block happens to be observed, it's still by definition a time critical one)
         self.istimecritical = 1
         for data in results:
            t1 = ephem.localtime(ephem.date(str(data[0])))
            t2 = ephem.localtime(ephem.date(str(data[1])))
            #if it overlaps with the time window, shrink the window at both ends (or only one end if possible)
            #(if and only if the window does not go outside the strict time window)
            #this is a simple solution - restricting the plausible times the block can be visited
            #to those of the time critical window, rather than trying to fit the block at the very start of
            #the window, which may not be possible to do (although, this may be desirable to 'enhance' the chances
            #of occurring in the optimisation of the queue)

            if(max(t1,self.dW1) <= min(t2,self.dW2)):
               self.time1 = t1
               self.time2 = t2
               self.timewindowactive=True
               #Always a sure bet?

               #No, could also start before a block to let whole window fall inside track!
               #no need to consider t1 before self.dW1 - because we then can't physically trim self.dW1
               if(t1 >= self.dW1):
                  self.dW1 = t1
               #t2 < self.dW2 (not t2 <= self.dW2 because if it was equal we wouldn't need to alter self.dW2)

               #Use 2014/11/30 , Husser
               #TC block in twilight!? It's not even got a time window for that night!

               if(t2 < self.dW2 and t2-self.dW1 >= timedelta(seconds=self.obstime)):
                  self.dW2 = t2
	       #I don't think this is needed at all, can't remember why it's in here!?
               #It creates a bug where the start time of a time critical observation can be outside the block point window (very bad!)
               #if(t2 >= self.dW2 and t2-timedelta(seconds=self.obstime) > self.dW1):
               #   self.dW2 = t2-timedelta(seconds=self.obstime)
               if(timedelta(seconds=self.obstime) >= t2-t1):
                  self.dW2 = self.dW1

               break
      cur.close()
 







      #M O O N

      #TODO LIST
      #May also want to disable moon check for all blocks (then have filters for illum fraction range to consider)
      
      #How to add option of considering grey blocks in dark time?
      #How to add option of considering bright blocks in dark time?
      #=>These can be solved using weights in objective function ? (but they have to be allowed in first)
      #=>So need a bypass moon option and THEN let the objective function handle it - e.g. downgrade blocks
      #not meant to be observed outside their normal time and keep the normal blocks' weighting unchanged.
      #Note the test for dark time (< 15.0) is strict.
      #If it were <= 15.0, it lets in grey time blocks...
      
      #N.B. We have to check there's still enough obstime left,
      #because the block point windows don't know about the moon!

      #Ignore moon if flag is set
      if(not self.istimecritical or (not self.ignoretcmoon and self.istimecritical)):
         #if the moon is not up
         if(mstart == 'NULL' and mend == 'NULL'):
            self.MoonOK = (float(self.minlunar) < 15.0)
         #else if the moon overlaps the block in any way...
         #to revisit: whether we add acqtime to dW1 here...
         elif(max(self.dW1,mstart) <= min(self.dW2+timedelta(seconds=self.obstime),mend)):
            #if the moon rises during the block
            if(mstart >= self.dW1 and mstart <= self.dW2+timedelta(seconds=self.obstime)):
               preOK = (float(self.minlunar) <  15.0)
               postOK = (float(self.minlunar) <= float(illum) <= float(self.maxlunar))
               #observe after the moon rises
               if(not preOK and postOK):
                  #Case (a)
                  if(mstart >= self.dW1 and mstart <= self.dW2):
                     self.dW1 = mstart
                     self.MoonOK = True
                  else: #(if mstart > self.dW2)
                     self.MoonOK = False
                 ##Case (b)
                 #if(mstart >= self.dW2 and mstart <= self.dW2+timedelta(seconds=self.obstime)):
                 ##Case (c)
                 #if(mstart >= self.dW1+timedelta(seconds=self.obstime) and mstart <= self.dW2+timedelta(seconds=self.obstime)):
                 ##Case (d)
                 #if(mstart >= self.dW2+timedelta(seconds=self.obstime)):
                 #   self.MoonOK = False
               #observe before the moon rises
               if(preOK and not postOK):
                  if(mstart >= self.dW1 and mstart <= self.dW2 and mstart-self.dW1 >= timedelta(seconds=self.obstime)):
                     self.dW2 = mstart
                     self.MoonOK = True
                  else:
                     self.MoonOK = False
                  if(mstart >= self.dW2 and mstart <= self.dW2+timedelta(seconds=self.obstime)):
                     if((self.dW2+timedelta(seconds=self.obstime))-mstart <= (self.dW2-self.dW1)):
                        self.dW2 = self.dW2 - ((self.dW2+timedelta(seconds=self.obstime))-mstart)
                        self.MoonOK = True
                     else:
                        self.MoonOK = False
               if(preOK and postOK):
                  self.MoonOK = True
                  #no need to modify dW1...dW2
               if(not preOK and not postOK):
                  self.MoonOK = False
                  #no need to modify dW1...dW2
            #if the moon sets during the block
            elif(mend >= self.dW1 and mend <= self.dW2+timedelta(seconds=self.obstime)):
               preOK = (float(self.minlunar) <= float(illum) <= float(self.maxlunar))
               postOK = (float(self.minlunar) <  15.0)
               #observe after the moon sets
               if(not preOK and postOK):
                  if(mend >= self.dW1 and mend <= self.dW2):
                     self.dW1 = mend
                     self.MoonOK = True
                  else:
                     self.MoonOK = False
               #observe before the moon sets
               if(preOK and not postOK):
                  if(mend >= self.dW1 and mend <= self.dW2 and mend - self.dW1 >= timedelta(seconds=self.obstime)):
                     self.dW2 = mend
                     self.MoonOK = True
                  else:
                     self.MoonOK = False
                  if(mend >= self.dW2 and mend <= self.dW2+timedelta(seconds=self.obstime)):# and mend-self.dW1 >= timedelta(seconds=self.obstime)):
                     if((self.dW2+timedelta(seconds=self.obstime))-mend <= (self.dW2-self.dW1)):
                        self.dW2 = self.dW2 - ((self.dW2+timedelta(seconds=self.obstime)) - mend)
                        self.MoonOK = True
                     else:
                        self.MookOK = False
               if(preOK and postOK):
                  self.MoonOK = True
                  #no need to modify dW1...dW2
               if(not preOK and not postOK):
                  self.MoonOK = False
                  #no need to modify dW1...dW2
            else: # moon covers entire block
               self.MoonOK = (float(self.minlunar) <= float(illum) <= float(self.maxlunar)) 
         #if the block is in the dark part of a night when the moon is up
         else:
            self.MoonOK = (float(self.minlunar) <  15.0)

      #fix up those blocks which have visibilities extending way past either twilight
      #Essentially a minimum start time...
      self.dW1=max(ti,self.dW1)
      #Essentially a maximum end time...
      self.dW2=min(tf,self.dW2)
      
      #N I G H T    B O U N D A R I E S

      #block overlaps with morning twilight - needs careful treatment
      if(tf >= self.dW1 and tf <= self.dW1+timedelta(seconds=self.obstime)):
         self.TwilightOK = False
      if(tf >= self.dW2 and tf <= self.dW2+timedelta(seconds=self.obstime)):
         if((self.dW2+timedelta(seconds=self.obstime))-tf <= (self.dW2-self.dW1)):
            self.dW2 = self.dW2 - ((self.dW2+timedelta(seconds=self.obstime)) - tf)
            self.TwilightOK = True
         else:
            self.TwilightOK = False

      self.b1=self.dW1
      self.b2=self.b1+timedelta(seconds=self.obstime) 
      self.ActualDuration = md.date2num(self.b2)-md.date2num(self.b1)
      self.Duration = md.date2num(self.dW2)-md.date2num(self.dW1)
      self.DurationSeconds = (self.dW2-self.dW1).total_seconds() 

   def GetChosenTimes(self):
      return (self.b1,self.b2)
   def HasBPW(self):
      #ICANHAZ BlockPointWindow?
      return self.bpw
   def GetBPW(self):
      #returns BlockPointWindow
      if(self.bpw):
         return (self.bpw1,self.bpw2)
   def GetWindowTimes(self):
      return (self.dW1,self.dW2)
   def GetStrictWindowTimes(self):
      return (self.MinStartTime,self.MaxEndTime)
   def GetChosenStart(self):
      return self.b1
   def GetChosenEnd(self):
      return self.b2
   def SetChosenStart(self,start):
      self.b1=start
      self.b2=self.b1+timedelta(seconds=self.obstime) 
   def GetWindowDuration(self):
      return self.WindowDuration

   def GetRects(self,showfull):#,y,dh):
   #return rectangles based on track type to plot in matplotlib
   #these are very basic for now
   #   return (Rectangle((self.dW1,y),self.WindowDuration,dh,color=self.colour,lw=2,alpha=0.2),Rectangle((self.dCW1,y),self.ActualDuration,dh,color=self.colour,lw=2,alpha=0.5),Rectangle((self.dCW1,y+dh),self.ActualDuration,-(0.1*dh),color=self.mooncolour,lw=1,alpha=0.5,ec='black'))
      rlist = [
            Rectangle((self.dW1,(4.0-self.priority)),self.Duration,1.0,color=self.colour,lw=2,alpha=0.1,url=self.url,picker=True),
            #Rectangle((self.dO1,(4.0-self.priority)),self.DurationO,1.0,color='magenta',lw=2,alpha=0.1,url=self.url,picker=True),
            Rectangle((self.b1,(4.0-self.priority)),self.ActualDuration,1.0,color=self.colour,lw=2,alpha=0.3,url=self.url,picker=True),
            Rectangle((self.b1,(4.0-self.priority)+1.0),self.ActualDuration,-(0.15*1.0),color=self.mooncolour,lw=1,alpha=1.0,ec='black',url=self.url,picker=True)
               ]
      if(self.istimecritical):
         rlist.append(
               Rectangle((self.b1,(4.0-self.priority)+0.85),self.ActualDuration,-(0.15*1.0),color='magenta',lw=1,alpha=1.0,ec='black',url=self.url,picker=True)
               )

      return rlist 
#this is old debugging stuff / other options, useful to leave it here...
 # (
 #          Rectangle((self.dO1,(4.0-self.priority)),self.DurationO,1.0,color='magenta',lw=2,alpha=0.1,url=self.url,picker=True),
 #          Rectangle((self.b1,(4.0-self.priority)),self.ActualDuration,1.0,color=self.colour,lw=2,alpha=0.3,url=self.url,picker=True),
 #          Rectangle((self.dW1,(4.0-self.priority)+1.0),self.ActualDuration,-(0.15*1.0),color=self.mooncolour,lw=1,alpha=1.0,ec='black',url=self.url,picker=True),
 #          Rectangle((self.dW1,(4.0-self.priority)),self.Duration,1.0,color=self.colour,lw=2,alpha=0.2,url=self.url,picker=True)
 #             )
 #    else:
 #       retur
 #    if(showfull):#and self.bpwDuration != None):
 #       #return (Rectangle((self.MinStartTime,(4.0-self.priority)),self.LatestPointDuration,1.0,color=self.colour,lw=2,alpha=0.2,url=self.url,picker=True),
 #       if(self.istimecritical):
 #          return (Rectangle((self.MinStartTime,(4.0-self.priority)),self.WindowDuration,1.0,color=self.colour,lw=2,alpha=0.1,url=self.url,picker=True),
 #             Rectangle((self.b1,(4.0-self.priority)),self.ActualDuration,1.0,color=self.colour,lw=2,alpha=0.5,url=self.url,picker=True),
 #             Rectangle((self.b1,(4.0-self.priority)+1.0),self.ActualDuration,-(0.15*1.0),color=self.mooncolour,lw=1,alpha=1.0,ec='black',url=self.url,picker=True),
 #             Rectangle((self.b1,(4.0-self.priority)+0.85),self.ActualDuration,-(0.15*1.0),color='magenta',lw=1,alpha=1.0,ec='black',url=self.url,picker=True)
 #            #Rectangle((self.dW1,(4.0-self.priority)),self.LatestPointDuration,1.0,color='purple',lw=2,alpha=0.3,url=self.url,picker=True),
 #             )
 #       else:
 #           # return (Rectangle((self.bpw1,(4.0-self.priority)),self.bpwDuration,1.0,color='pink',lw=2,alpha=0.6,url=self.url,picker=True),
 #             #Rectangle((self.MinStartTime,(4.0-self.priority)),self.WindowDuration,1.0,color=self.colour,lw=2,alpha=0.1,url=self.url,picker=True),
 #           #       )
 #          return (Rectangle((self.dW1,(4.0-self.priority)),self.Duration,1.0,color=self.colour,lw=2,alpha=0.1,url=self.url,picker=True),
 #             Rectangle((self.dW1,(4.0-self.priority)),self.Duration,1.0,color=self.colour,lw=2,alpha=0.5,url=self.url,picker=True),
 #             #Rectangle((self.dW1,(4.0-self.priority)+1.0),self.ActualDuration,-(0.15*1.0),color=self.mooncolour,lw=1,alpha=1.0,ec='black',url=self.url,picker=True)
 #            #Rectangle((self.dW1,(4.0-self.priority)),self.LatestPointDuration,1.0,color='purple',lw=2,alpha=0.3,url=self.url,picker=True),
 #             )

 #       #original version
         #return (Rectangle((self.dW1,(4.0-self.priority)),self.WindowDuration,1.0,color=self.colour,lw=2,alpha=0.2,url=self.url,picker=True),Rectangle((self.dCW1,(4.0-self.priority)),self.ActualDuration,1.0,color=self.colour,lw=2,alpha=0.5,url=self.url,picker=True),Rectangle((self.dCW1,(4.0-self.priority)+1.0),self.ActualDuration,-(0.15*1.0),color=self.mooncolour,lw=1,alpha=1.0,ec='black',url=self.url,picker=True))
#     
#     else:
#        if(self.istimecritical):
#           return (Rectangle((self.b1,(4.0-self.priority)),self.ActualDuration,1.0,color=self.colour,lw=2,alpha=0.5,url=self.url,picker=True),
#              Rectangle((self.b1,(4.0-self.priority)+1.0),self.ActualDuration,-(0.15*1.0),color=self.mooncolour,lw=1,alpha=1.0,ec='black',url=self.url,picker=True),
#              Rectangle((self.b1,(4.0-self.priority)+0.85),self.ActualDuration,-(0.15*1.0),color='magenta',lw=1,alpha=1.0,ec='black',url=self.url,picker=True)
#              )
#        else:
#           return (Rectangle((self.b1,(4.0-self.priority)),self.ActualDuration,1.0,color=self.colour,lw=2,alpha=0.5,url=self.url,picker=True),
#              Rectangle((self.b1,(4.0-self.priority)+1.0),self.ActualDuration,-(0.15*1.0),color=self.mooncolour,lw=1,alpha=1.0,ec='black',url=self.url,picker=True))
         #original version
         #return (Rectangle((self.dCW1,(4.0-self.priority)),self.ActualDuration,1.0,color=self.colour,lw=2,alpha=0.5,url=self.url,picker=True),Rectangle((self.dCW1,(4.0-self.priority)+1.0),self.ActualDuration,-(0.15*1.0),color=self.mooncolour,lw=1,alpha=1.0,ec='black',url=self.url,picker=True))

      #self.rwindow = Rectangle((self.dW1,y),self.WindowDuration,dh,color=self.colour,lw=2,alpha=0.2)
      #self.rchosen = Rectangle((self.dCW1,y),self.ActualDuration,dh,color=self.colour,lw=2,alpha=0.5)
      #self.rmoon = Rectangle((self.dCW1,y+dh),self.ActualDuration,-(0.1*dh),color=self.mooncolour,lw=1,alpha=0.5,ec='black')
      #return (self.rwindow,self.rchosen,self.rmoon)

   #work out the distance from the moon for this block
   #this should be incorporated elsewhere when saving the SQL query with calculated extras to a FITS table
   #for now it can go here
   def CalcMoonDist(self,mra,mdec):
         dtor = np.pi/180
         self.moondist = 180.0/np.pi*(np.arccos(np.cos(self.dec*dtor)*np.cos(mdec*dtor)*np.cos((self.ra-mra)*dtor) + np.sin(self.dec*dtor)*np.sin(mdec*dtor)))
