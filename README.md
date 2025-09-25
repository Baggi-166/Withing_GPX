The python script generate gpx files based on your personnal archive, download from Withings servers (export menu).
The Zip file must be unzip in a folder.
When lunch the script, it will ask for the folder then :
 - based on activities.csv file
   it will ask for a activity type to select (running, walking etc ...)
 - it will ask for a date range
 - for each activity from the specified type, on the data range a gpx will be created, in a folder named /export.

BAsed on the assumption that the hr point are the  anchor point,
for each data in raw_hr.csv file during the activity duration, a timestamp is added on the gpx with the raw value of HR
for each point, the script, 
  - based on raw_latitude and raw_longitude files, extrapolate a position (smooth on a 10s windows to smooth localisation error), add a position
  - based on core body file, extrapolate a body temperature in a atemp fields (to be improved, not working on strava)
  - based on raw step file, indicate the cadence;

