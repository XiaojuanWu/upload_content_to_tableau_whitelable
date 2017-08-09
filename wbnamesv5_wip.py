import psycopg2
import os
import sys
import getopt
import shutil
import re
import time
import itertools
import glob
import zipfile
import subprocess

cmd_opt = '  -o --save-db-password'
# tocName = 'TuneBI_v1.6.0_TableOfContents_Template_Packaged.twb'

# Start time
t_0 = time.time()

version = '1.5.1'
# userid/passwd, and template files, will be populated from config
user = pw = db_user = db_pw = wb_name = ''
templates = []
twbs = []
tdss = []
# add in twbx
twbx = []

# State wb's to be updated
# wbid = ['4', '5', '6', '7', '8', '9', '10', '12', '13', '18', '19', '20']
wbid = ['23', '24', '25', '26', '27', '28', '29', '31', '32', '18', '19', '20']

wb = ['UserAcquisition', 'Engagement', 'Retention', 'LTV', 'Re-Engagement', 'Geography', 'SubLevel', 'TrafficQuality', 'ExecutiveSummary']

# Set up getwbid() iteration
wbiter = itertools.izip_longest(wb, wbid)

# make sure template dir 3444 exists
if not os.path.exists("3444"):
    raise NameError("dir '3444' does not exist")
elif not os.path.isdir("3444"):
    raise NameError("'3444' exists, but is not a directory.")

# Show database name
conn_string = "host='p-p3tableaum01-1a.use01.plat.priv' dbname='workgroup' port='8060' user='readonly' password='dDCAJP9pSpfw'"
print "Database : %s" % (conn_string)

# State the parameters to be uploaded
parms = len(sys.argv)
advertiser_name = '0000'
advertiser_id = '0000'
config = 'advertiser_toc.config'
global run_tabcmd

try:
    opts, args = getopt.getopt(sys.argv[1:], "h", ["advertiser_id=", "advertiser_name=", "run_tabcmd="])
except getopt.GetoptError:
    printUsage(sys.argv[0])
    sys.exit(2)


for opt, arg in opts:
    if opt == '-h':
        printUsage(sys.argv[0])
        sys.exit()
    elif opt in ("--advertiser_name"):
        advertiser_name = arg
    elif opt in ("--advertiser_id"):
        advertiser_id = arg
    elif opt in ("--run_tabcmd"):
        run_tabcmd = arg.lower()
        parms -= 1

# cover run_tabcmd=y/Y
if run_tabcmd == "y":
    run_tabcmd = "yes"

# print out parameters
print 'advertiser_id ', advertiser_id
print 'advertiser_name ', advertiser_name
print 'run_tabcmd ', run_tabcmd
print 'version ', version

# Shorten variable names
adv_id = advertiser_id
adv_nm = advertiser_name
adv_name = advertiser_name

# create the advertiser directory if not already exists
if os.path.exists(adv_id) and not os.path.isdir(adv_id):
    raise NameError(adv_id + " exists, but is not a directory.")
elif not os.path.exists(adv_id):
    os.makedirs(adv_id)
    os.makedirs(adv_id + '/Image')
    # Copy contents of 3444/Image to new adv_id/Image

    for item in os.listdir('3444/Image'):
        s = os.path.join('3444/Image', item)
        d = os.path.join(adv_id + '/Image', item)
        if os.path.isdir(s):
            shutil.copytree(s, d, symlinks, ignore)
        else:
            shutil.copy2(s, d)
else:
    # Check to see if Image folder already exists
    if adv_id != '3444':
        if os.path.exists(adv_id + '/Image'):
            shutil.rmtree(adv_id + '/Image')
        # Create new Image folder
        os.makedirs(adv_id + '/Image')
        # Copy contents of 3444/Image to new adv_id/Image
        for item in os.listdir('3444/Image'):
            s = os.path.join('3444/Image', item)
            d = os.path.join(adv_id + '/Image', item)
            if os.path.isdir(s):
                shutil.copytree(s, d, symlinks=False)
            else:
                shutil.copy2(s, d)
    else:
        if os.path.exists(adv_id + '/uploaded/Image'):
            shutil.rmtree(adv_id + '/uploaded/Image')
        # Create new Image folder
        os.makedirs(adv_id + '/uploaded/Image')
        # Copy contents of 3444/Image to new adv_id/Image
        for item in os.listdir('3444/Image'):
            s = os.path.join('3444/Image', item)
            d = os.path.join(adv_id + '/uploaded/Image', item)
            if os.path.isdir(s):
                shutil.copytree(s, d, symlinks=False)
            else:
                shutil.copy2(s, d)

# double check the new dir
if not os.path.isdir(adv_id):
    raise NameError("dir '" + adv_id + "' does not exist.")


def getConfiguration(config):
    # read in userid, passwd, valid template, etc
    global user, pw, db_user, db_pw, wb_name, templates, twbs, twbx
    if not os.path.exists(config):
        raise NameError("file " + config + " does not exist")

    with open(config, "r") as f:
        for line in f:
            # strip leading/trailing white spaces, and quote characters
            line = fixQuote(line, '')

            # skip empty or commented lines
            if not line or line[0] == '#':
                continue

            m_lu = re.match(r'(login_user *= *)(.*)', line)
            m_lp = re.match(r'(login_pw *= *)(.*)', line)
            m_du = re.match(r'(db_username *= *)(.*)', line)
            m_dp = re.match(r'(db_password *= *)(.*)', line)
            m_wb = re.match(r'(wb_name *= *)(.*)', line)
            m_toc = re.match(r'(toc_name *= *)(.*)', line)

            if m_lu:
                user = m_lu.group(2)
                user = fixQuote(user, '"')
            elif m_lp:
                pw = m_lp.group(2)
                pw = fixQuote(pw, '"')
            elif m_du:
                db_user = m_du.group(2)
                db_user = fixQuote(db_user, '"')
            elif m_dp:
                db_pw = m_dp.group(2)
                db_pw = fixQuote(db_pw, '"')
            elif m_wb:
                wb_name = m_wb.group(2)
                wb_name = fixQuote(wb_name, '"')
            elif m_toc:
                tocName = m_toc.group(2)
                # tocName = fixQuote(tocName, '"')
            else:
                if line.endswith('.twb'):
                    twbs.append(line)
                    templates.append(line)
                elif line.endswith('.twbx'):
                    twbx.append(line)
                    templates.append(line)
                elif line.endswith('.tds'):
                    tdss.append(line)
                    templates.append(line)

    if not user or not pw or not db_user or not db_pw or not wb_name:
        raise NameError("Please check the advertiser.config, at least one parm is not correct.")
    return tocName


def fixQuote(a, q):
    # strip leading/trailing white spaces, and quote characters
    a = a.strip()
    a = a.strip("'")
    a = a.strip('"')
    a = a.strip()
    a = q + a + q
    return a


def printUsage(name):
    # display a usage hint
    print name, '[--advertiser_id=<id> --advertiser_name=<name>]'
    print name, 'version', version


def getwbid(key1, key):
    # Where are we connecting
    conn_string = "host='p-p3tableaum01-1a.use01.plat.priv' dbname='workgroup' port='8060' user='readonly' password='dDCAJP9pSpfw'"

    # print the connection string we will use to connect
    print "Connecting to database..."
    patternkey = key
    print "Regex key -> " + patternkey

    # get a connection, if a connect cannot be made an exception will be raised here
    conn = psycopg2.connect(conn_string)

    # conn.cursor will return a cursor object, you can use this cursor to perform the desired query
    cursor = conn.cursor()

    # execute Query
    cursor.execute("SELECT w.name, w.id\
                    FROM _sites s LEFT JOIN _workbooks w ON w.site_id = s.id\
                    WHERE w.name is not null and s.url_namespace = %s and w.name = %s", (adv_nm, key1,))

    # retrieve the records from the database
    rows = cursor.fetchone()
    name, id = rows
    print 'Workbook -> ' + name
    print 'ID -> ' + str(id)

    return id


def getprojid(key1, key):
    # Where are we connecting
    conn_string = "host='p-p3tableaum01-1a.use01.plat.priv' dbname='workgroup' port='8060' user='readonly' password='dDCAJP9pSpfw'"

    # print the connection string we will use to connect
    print "Connecting to database..."
    patternkey = key

    # get a connection, if a connect cannot be made an exception will be raised here
    conn = psycopg2.connect(conn_string)

    # conn.cursor will return a cursor object, you can use this cursor to perform the desired query
    cursor = conn.cursor()

    # execute Query
    cursor.execute("SELECT p.id\
                    FROM _sites s\
                    LEFT JOIN _projects p ON p.site_id = s.id\
                    LEFT JOIN _workbooks w ON w.site_id = s.id\
                    WHERE w.name is not null and s.url_namespace = %s and p.name = 'default'\
                    GROUP BY p.id", (adv_nm,))

    # retrieve the records from the database
    rows = cursor.fetchone()
    id = rows[0]
    print 'Workbook -> All views'

    return id


def getsbid(key1, key):
    # Where are we connecting
    conn_string = "host='p-p3tableaum01-1a.use01.plat.priv' dbname='workgroup' port='8060' user='readonly' password='dDCAJP9pSpfw'"

    # print the connection string we will use to connect
    print "Connecting to database..."
    patternkey = key
    print "Regex key -> " + patternkey

    # get a connection, if a connect cannot be made an exception will be raised here
    conn = psycopg2.connect(conn_string)

    # conn.cursor will return a cursor object, you can use this cursor to perform the desired query
    cursor = conn.cursor()

    # execute Query
    cursor.execute("SELECT p.id\
                    FROM _sites s\
                    LEFT JOIN _projects p ON p.site_id = s.id\
                    LEFT JOIN _workbooks w ON w.site_id = s.id\
                    WHERE w.name is not null and s.url_namespace = %s and p.name = 'Sandbox'\
                    GROUP BY p.id", (adv_nm,))

    # retrieve the records from the database
    rows = cursor.fetchone()
    id = rows[0]
    print 'Workbook -> Sandbox'
    print 'ID -> ' + str(id)

    return id


def update(key1, key, tocName, outfile):
    # Id new twb to update
    if adv_id != '3444':
        twb_temp = advertiser_id + '/' + tocName
    else:
        twb_temp = advertiser_id + '/uploaded/' + tocName
    try:  # If the id exists, proceed with updating URL
        # Bring in workbook id
        if key not in ['18', '19', '20']:
            id = getwbid(key1, key)
        elif key in ['20', '19']:
            id = getsbid(key1, key)
        elif key == '18':
            id = getprojid(key1, key)

        # Create the pattern object
        rekey = key
        rawpattern = "id='" + re.escape(rekey) + "'(.*?)url='(.*?)'"
        print rawpattern
        print 'Postgres ID =', id

        pattern = re.compile(rawpattern)

        # What you want to replace the pattern with
        if key not in ['18', '19', '20']:
            substitution = "id='%s' is-centered='0' is-scaled='1' param='Image/Blank Spaces-01.png' type='bitmap' url='http://bi.tune.com/#/site/%s/workbooks/%s/views?order=name:asc'" % (key, advertiser_name, id)
        elif key == '19':
            if advertiser_name == 'TuneBIDemo':
                substitution = "id='%s' is-centered='0' is-scaled='1' param='Image/Blank Space.png' type='bitmap' url='http://bi.tune.com/t/%s/authoring/Sandbox_0/SandboxSheet1'" % (key, advertiser_name)
            else:
                substitution = "id='%s' is-centered='0' is-scaled='1' param='Image/Blank Space.png' type='bitmap' url='http://bi.tune.com/t/%s/authoring/Sandbox/SandboxSheet1'" % (key, advertiser_name)
        elif key == '20':
            substitution = "id='%s' is-centered='0' is-scaled='1' param='Image/Blank Space.png' type='bitmap' url='http://bi.tune.com/#/site/%s/projects/%s/workbooks?order=name:asc'" % (key, advertiser_name, id)
        elif key == '18':
            substitution = "id='%s' is-centered='0x' is-scaled='1' param='Image/Blank Space.png' type='bitmap' url='http://bi.tune.com/#/site/%s/views?order=name:asc'" % (key, advertiser_name)

        # Open the source file and read it
        fh = file(twb_temp, 'r')
        subject = fh.read()
        fh.close()

        # Do the replace
        result = pattern.sub(substitution, subject)

        # Write the file
        f_out = file(twb_temp, 'w')
        f_out.write(result)
        f_out.close()

    except:  # If the id doesn't exist, point it to 'All Views'
        # Create the pattern object
        rekey = key
        rawpattern = "id='" + re.escape(rekey) + "'(.*?)url='(.*?)'"
        print rawpattern

        pattern = re.compile(rawpattern)

        substitution = "id='%s' is-centered='0' is-scaled='1' param='Image/Blank Space.png' type='bitmap' url='http://bi.tune.com/#/site/%s/projects/%s/workbooks?order=name:asc'" % (key, advertiser_name, '13')

        # Open the source file and read it
        fh = file(twb_temp, 'r')
        subject = fh.read()
        fh.close()

        # Do the replace
        result = pattern.sub(substitution, subject)

        # Write the file
        f_out = file(twb_temp, 'w')
        f_out.write(result)
        f_out.close()

    if re.search(rawpattern, subject):
        print 'Match found'
        print outfile + ' updated'
    else:
        print 'Match not found'
        print outfile + ' unchanged'


def repack():
    # Repackage new twb in new folder
    print '##### Now in repack function'
    if adv_id != '3444':
        target_folder = adv_id
    else:
        target_folder = adv_id + '/uploaded'

    for name in glob.glob(target_folder + '/*TableOfContents*.twb'):
        base_name = os.path.basename(name)
        file_name, file_ext = os.path.splitext(base_name)
        twbx_temp = target_folder + '/' + file_name + '.temp'
        os.makedirs(twbx_temp)
        os.makedirs(twbx_temp + '/Image')
        shutil.move(name, twbx_temp)
        # copies Image folder to temp directory
        image_base = target_folder + '/Image'
        for item in os.listdir(image_base):
            s = os.path.join(image_base, item)
            d = os.path.join(twbx_temp + '/Image', item)
            if os.path.isdir(s):
                shutil.copytree(s, d, symlinks, ignore)
            else:
                shutil.copy2(s, d)

    # look for folders with .temp zip it
    for name in glob.glob(target_folder + '/*TableOfContents*.temp'):
        base_name = os.path.basename(name)
        file_name, file_ext = os.path.splitext(base_name)
        shutil.make_archive(target_folder + '/' + file_name, 'zip', name)
        shutil.rmtree(name)

    # change the .zip extension to .twbx
    for name in glob.glob(target_folder + '/*TableOfContents*.zip'):
        base_name = os.path.basename(name)
        file_name, file_ext = os.path.splitext(base_name)
        print file_name + '.twbx has been repackaged'

        if os.path.exists(target_folder + '/' + file_name + '.twbx'):
            os.remove(target_folder + '/' + file_name + '.twbx')
            print 'Removed old version of ' + file_name + '.twbx'
            os.rename(name, target_folder + '/' + file_name + '.twbx')
            shutil.rmtree(target_folder + '/Image')
        else:
            os.rename(name, target_folder + '/' + file_name + '.twbx')
            # shutil.rmtree(target_folder + '/Image')


def runTabCmd(cmd):
    # run tableau command and display the output
    # may need to parse the returned information
    print cmd

    if run_tabcmd == 'yes':
        p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        for line in p.stdout.readlines():
            print line,
        retval = p.wait()

        # is this necessary?
        time.sleep(2)


def doPublish(adv_dir):
    # publish to workbook and data source
    print 'twbs: ',  twbs
    for wb in twbs:
        wb_split = wb.split('_')

        if len(wb_split) == 5 and re.search('Packaged', wb):  # check the length of the file
            wb = wb + 'x'
            wb = os.path.join(adv_dir, wb)
            print wb
            wb_name = wb_split[2].split('.')[0]  # if it's the proper length grab index 2, should be the name
            runTabCmd('tabcmd publish ' + wb + ' -n ' + wb_name + ' --db-username ' + db_user + ' --db-password ' + db_pw + cmd_opt)

        elif len(wb_split) == 5 and re.search('Packaged', wb) == None:  # check the le  ngth of the file
            wb_name = wb_split[2].split('.')[0]  # if it's the proper length grab index 2, should be the name
            wb = os.path.join(adv_dir, wb + 'x')
            print wb
            runTabCmd('tabcmd publish ' + wb + ' -n ' + wb_name + ' --db-username ' + db_user + ' --db-password ' + db_pw + cmd_opt)

        else:
            wb_name = wb + 'x'
            wb = os.path.join(adv_dir, wb + 'x')
            runTabCmd('tabcmd publish ' + wb + ' -n ' + wb_name + ' --db-username ' + db_user + ' --db-password ' + db_pw + cmd_opt)

        time.sleep(30)  # add 60 sec to let tableau server breathe

    for ds in tdss:
        ds = os.path.join(adv_dir, ds)
        runTabCmd('tabcmd publish ' + ds + ' --db-username ' + db_user + ' --db-password ' + db_pw + cmd_opt)


def singleAdvertiser(adv_id, adv_name):

    # create a new advertiser or update a existing advertiser
    if not os.path.exists("advertiser.info"):
        raise NameError("file 'existing_advertiser' does not exist")

    # construct a advertiser ID
    advertiser = "advertiser_" + adv_id + "_tetris"

    # check if this is a new advertiser
    is_new_adv = False
    if advertiser not in open("advertiser.info").read():
        is_new_adv = True
        f = open("advertiser.info", "a")
        print >> f, adv_id, advertiser, adv_name
        f.close()

    # update the workbooks
    # updateWorkbooks(adv_id, from_port, to_port, sharded_schema)

    # log into server
    runTabCmd('tabcmd login -s p-p3tableaum01-1a.use01.plat.priv -u ' + user + ' -p ' + pw + ' --cookie')

    # new advertiser set up
    if is_new_adv:
        # create a site for the customer, limit the site_admin power
        runTabCmd('tabcmd createsite "' + adv_name + '" --no-site-mode')

        # log out and log back into site
        runTabCmd('tabcmd logout')
        runTabCmd('tabcmd login -s p-p3tableaum01-1a.use01.plat.priv -t "' + adv_name + '" -u ' + user + ' -p ' + pw + ' --cookie')

        # create site users from users.csv
        # runTabCmd('tabcmd createsiteusers "users.csv"' + cmd_opt)
    else:
        # log into server/site
        runTabCmd('tabcmd login -s p-p3tableaum01-1a.use01.plat.priv -t "' + adv_name + '" -u ' + user + ' -p ' + pw + ' --cookie')
    print 'Publishing'
    # publish workbook, and datasources
    if adv_id != '3444':
        doPublish(adv_id)
    else:
        doPublish(adv_id + '/uploaded')

    # log out
    runTabCmd('tabcmd logout')


def main():
    # read in the configuration
    tocName = getConfiguration(config)

    # new code added to unzip the packaged workbooks before upload
    print 'Unpacking twbx files to start update process...'
    for name in glob.glob('3444/*TableOfContents*.twbx'):
        file_name, file_ext = os.path.splitext(name)
        os.rename(name, file_name + '.zip')
    for name in glob.glob('3444/*TableOfContents*.zip'):
        zip = zipfile.ZipFile(name, 'r')
        print 'Extracting...'
        zip.extractall('3444')
    # Define soruce and update files
    source = '3444/' + tocName
    if adv_id != '3444':
        outfile = advertiser_id + '/' + tocName
    else:
        outfile = advertiser_id + '/uploaded/' + tocName
    # Copy source file
    shutil.copyfile(source, outfile)

    for key1, key in wbiter:
        update(key1, key, tocName, outfile)

    print 'Table of Contents successfully generated'
    # Repackage the files
    repack()

    # Upload the workbooks
    singleAdvertiser(adv_id, adv_name)

    # Elapsed time in seconds
    t_1 = time.time()
    t_elapsed = t_1 - t_0
    print str(t_elapsed) + ' sec'

if __name__ == "__main__":
    # Iterate to update the new file
    main()
