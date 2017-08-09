Upload Process README

Before you start
    read all of the steps first
    install Anaconda with pyton 2.7
    load the Conda env file tableaudev-win.yml
    set up files according to checklist in step 5
    make sure you can run in Windows

Step 1 - make sure you have the most recent versions of the workbooks to be
uploaded in the 3444 folder. Sandboxes require the datasources be uploaded at
the same time and that a Sandbox project be created on the site.

Step 2 - Ensure that the twb and twbx versions of the workbooks have the same
name and follow the naming convention:
    TuneBI_vx.x.x_workbookname_advID_Packaged.twb(x)

Step 3 - Check advertiser.config to see that the names of the files are listed
with .twb as the extension. Also check advertiser_toc.config to ensure that
you are uploading the correct version of the table of contents. Both files
need to have your login credentials entered into them.

Step 4 - Check advertiser_batch.csv to ensure that the advertisers to be
uploaded are listed with their advertiser ID (name,ID) - also make sure not to
upload to 3444 (causes errors with the files because it's the template).
Optionally - you can add a 3rd field after the advertiser ID with one of 3
values:
    wb - to only upload workbooks to the site
    toc - to only upload a new/refreshed table of contents
    full - this uploads the new workbooks and a table of contents
    * Note * you can also leave it blank and the batch_upload.py script will
    default to content only.

Step 5 - file checklist
    advertiservX_stable.py
    batch_upload.py
    advertiser_batch.csv
    advertiser.config
    advertuser_toc.config
    advertiser.info
    whiteLabel.config
    wbnamesvX_stable.py
    3444 (template folder)

Step 6 - Skip to 8 if not whitelabeling - place the png of the logo of the
client to be whitelabeled into the Images folder in 3444. Must be named as
advertiser id to be picked up by the script.

Step 7 - Update the whiteLabel.config with the 5 colors (hex values) of your
choosing. (ID,header color,header font color,subhead color,subhead font color,
header bottom border)

Step 8 - Ensure that python, conda, and postgres are properly configured in winodws and
 that the python package 'psycopg2' is correctly installed.

Step 9 - Run 'python batch_upload.py' in Windows to upload content

Step 10 - before running every advertiser on the list, make sure to test the
new content by running a batch of only one large advertiser to ensure there are
no anomalies in the queries.
