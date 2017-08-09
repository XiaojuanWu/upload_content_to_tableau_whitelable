import getopt
import os
import re
import subprocess
import sys
import time
import zipfile
import glob
import shutil
import csv
import psycopg2
import datetime

# common options for tabcmd
# each tabcmd can have it own opt string if necessary
cmd_opt = '  -o --save-db-password'

# for debugging
# the tabcmd commands be just printed, or actual run
version = '1.0.4'
run_tabcmd = 'yes'

# userid/passwd, and template files, will be populated from config
user = pw = db_user = db_pw = wb_name = ''
templates = []
twbs = []
tdss = []
# add in twbx
twbx = []


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
        raise NameError(
            "Please check the advertiser.config, at least one parm is not correct.")


def fixQuote(a, q):
    # strip leading/trailing white spaces, and quote characters
    a = a.strip()
    a = a.strip("'")
    a = a.strip('"')
    a = a.strip()
    a = q + a + q
    return a


def singleAdvertiser(adv_id, adv_name, from_port, to_port, sharded_schema, update_sandbox):
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
        print >> f, adv_id, advertiser, sharded_schema, adv_name
        f.close()

    if adv_id != '3444':
        # update the workbooks
        updateWorkbooks(adv_id, adv_name, from_port, to_port, sharded_schema)
    else:
        None

    # Check to see if it is necessary to create a Sandbox

    # makeSb(adv_name, adv_id)

    # log into server
    runTabCmd('tabcmd login -s p-p3tableaum01-1a.use01.plat.priv -u ' +
              user + ' -p ' + pw + ' --cookie')

    # new advertiser set up
    if is_new_adv:
        # create a site for the customer, limit the site_admin power
        runTabCmd('tabcmd createsite "' + adv_name + '" --no-site-mode')

        # log out and log back in to create Sandbox project
        runTabCmd('tabcmd logout')
        runTabCmd('tabcmd createproject -n "Sandbox"')

        # log out and log back into site
        runTabCmd('tabcmd logout')
        runTabCmd('tabcmd login -s p-p3tableaum01-1a.use01.plat.priv -t "' +
                  adv_name + '" -u ' + user + ' -p ' + pw + ' --cookie')

        # create site users from users.csv
        # runTabCmd('tabcmd createsiteusers "users.csv"' + cmd_opt)
    else:
        # log into server/site
        runTabCmd('tabcmd login -s p-p3tableaum01-1a.use01.plat.priv -t "' +
                  adv_name + '" -u ' + user + ' -p ' + pw + ' --cookie')

    # publish workbook, and datasources
    doPublish(adv_id, adv_name, update_sandbox)

    # log out
    runTabCmd('tabcmd logout')


def allAdvertisers(from_port, to_port):
    # loop through the existing advertisers
    with open('advertiser.info', "r") as f:
        for line in f:
            # remove leading/trailing white spaces
            line = line.strip()

            # skip comments, commented lines
            if line[0] == '#':
                continue

            # adv_name may have space in it
            # info = line.split()
            info = re.match(r'(\d*) *(\w*) *(\w*) *(.*)', line)
            adv_id = info.group(1)
            sharded = info.group(3)
            adv_name = info.group(4)

            # update the workbooks
            updateWorkbooks(adv_id, adv_name, from_port, to_port, sharded)

            # log into server/site
            runTabCmd('tabcmd login -s p-p3tableaum01-1a.use01.plat.priv -t "' +
                      adv_name + '" -u ' + user + ' -p ' + pw + ' --cookie')

            # publish workbooks and data sources with overwrite
            doPublish(adv_id)

            # log out
            runTabCmd('tabcmd logout')


# Convert the hex codes in Whitelable.congfig to rgb
def hex_to_rgb(value):
    value = value.lstrip('#')
    lv = len(value)
    return tuple(int(value[i:i + lv // 3], 16) for i in range(0, lv, lv // 3))


# Convert rgb back to hex
def rgb_to_hex(rgb):
    return '#%02x%02x%02x' % rgb


# Get the HSV value from rgb
def rgb_to_hsv(r, g, b):
    r, g, b = r / 255.0, g / 255.0, b / 255.0
    mx = max(r, g, b)
    mn = min(r, g, b)
    df = mx - mn
    if mx == mn:
        h = 0
    elif mx == r:
        h = (60 * ((g - b) / df) + 360) % 360
    elif mx == g:
        h = (60 * ((b - r) / df) + 120) % 360
    elif mx == b:
        h = (60 * ((r - g) / df) + 240) % 360
    if mx == 0:
        s = 0
    else:
        s = df / mx
    v = mx
    return h, s, v


def updateWorkbooks(adv_id, adv_name, from_port, to_port, sharded_schema):
    # This function creates/updates the workbooks for a customer

    # make sure template dir 3444 exists
    if not os.path.exists("3444"):
        raise NameError("dir '3444' does not exist")
    elif not os.path.isdir("3444"):
        raise NameError("'3444' exists, but is not a directory.")

    # create the advertiser directory if not already exists

    if os.path.exists(adv_id) and not os.path.isdir(adv_id):
        raise NameError(adv_id + " exists, but is not a directory.")
    elif not os.path.exists(adv_id):
        os.makedirs(adv_id)
        os.makedirs(adv_id + '/Image')
        print 'Creating folder for ' + adv_id

        # Copy contents of 3444/Image to new adv_id/Image
        for item in os.listdir('3444/Image'):
            s = os.path.join('3444/Image', item)
            d = os.path.join(adv_id + '/Image', item)
            if os.path.isdir(s):
                shutil.copytree(s, d, symlinks=False)
            else:
                shutil.copy2(s, d)

        # Copy contents of 3444/Image to new adv_id/Data
        os.makedirs(adv_id + '/Data')

        for data_item in os.listdir('3444/Data'):
            data_s = os.path.join('3444/Data', data_item)
            data_d = os.path.join(adv_id + '/Data', data_item)
            if os.path.isdir(data_s):
                shutil.copytree(data_s, data_d, symlinks=False)
            else:
                shutil.copy2(data_s, data_d)
    else:
        # Check to see if Image folder already exists
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
        # Check to see if Data folder already exists
        if os.path.exists(adv_id + '/Data'):
            shutil.rmtree(adv_id + '/Data')
        # Create new Data folder
        os.makedirs(adv_id + '/Data')
        # Copy contents of 3444/Image to new adv_id/Data
        for data_item in os.listdir('3444/Data'):
            data_s = os.path.join('3444/Data', data_item)
            data_d = os.path.join(adv_id + '/Data', data_item)
            if os.path.isdir(data_s):
                shutil.copytree(data_s, data_d, symlinks=False)
            else:
                shutil.copy2(data_s, data_d)

    # double check the new dir
    if not os.path.isdir(adv_id):
        raise NameError("dir '" + adv_id + "' does not exist.")

    # process a list of files in 3444 by replacing 3444 with adv_id, and making the port changes
    # get template file names, and construct new ones for new customer
    for template_file in templates:
        # the advertiser in template is hardcoded to 3444, Jim doesn't want it
        # be passed in argument for now
        advertiser = "advertiser_" + adv_id + "_tetris"
        new_file = adv_id + "/" + template_file
        template_file = "3444/" + template_file

        # read template file, 'rb' making sure window will not chnaging the eol
        # char
        fi = open(template_file, "rb")
        bs = fi.read()
        fi.close()

        # replace hardcode id '3444 advertiser[_id]' in select with adv_id
        rs = re.compile("(\D)3444( advertiser)(|_id)(\W)")

        # replace "port='from_port'" to "port='11101'"
        pe = "(port *= *')" + from_port + "(')"
        rp = re.compile(pe)

        # make port change if necessary
        bo = bs
        if from_port != '0000' and to_port != '0000' and from_port != to_port:
            bo = rp.sub("\g<1>" + to_port + "\g<2>", bs)

        # make select clause changes
        bs = rs.sub("\g<1>" + adv_id + "\g<2>\g<3>\g<4>", bo)

        # make where clause change
        sharded_schema = 'yes'
        if sharded_schema == 'yes':
            # the new advertiser in the sharded advertiser schemas
            rt = re.compile("(\Wdbname\s*=\s*)'.+_tetris'")
            bx = rt.sub("\g<1>'" + advertiser + "'", bs)
            # replace hardcode filter "[qulified.]advertiser_id = 3444" with
            # "1=1"
            rf = re.compile("(\W)([\w.]*advertiser_id\s*=\s*)3444(\D)")
            bo = rf.sub("\g<1>" + "1=1" + "\g<3>", bx)
        else:
            # replace hardcode id value 3444 in filter with adv_id
            rf = re.compile("(\W)(advertiser_id\s*=\s*)3444(\D)")
            bo = rf.sub("\g<1>\g<2>" + adv_id + "\g<3>", bs)

        # whitelabel the file if the logo exists in the Image folder
        if os.path.isfile('3444/Image/' + adv_id + '.png'):
            # Create the pattern objects
            lp = re.compile(r"TUNE_VisualMark_White")
            ls = adv_id

            # Do the replace for the logo
            lr = lp.sub(ls, bo)

            wl = open('whitelabel.config', 'rb')
            reader = csv.reader(wl)
            # Assign colors from file
            for row in reader:
                color_id = row[0]
                color1 = row[1]
                color2 = row[2]
                color3 = row[3]
                color4 = row[4]

                if color_id == adv_id:
                    # Create pattern for color
                    cs1 = color1
                    cs2 = color2
                    cs3 = color3
                    cs4 = color4
                    break

            # Do the replace for the main color
            cp1 = re.compile(r"#002549")
            cr1 = cp1.sub(cs1, lr)
            print 'Primary Color - ' + cs1
            # Replace for the main font color
            cp2 = re.compile(r"#fdffff")
            cr2 = cp2.sub(cs2, cr1)
            print 'Primary Font - ' + cs2
            # Replace for the secondary color
            cp3 = re.compile(r"#163858")
            cr3 = cp3.sub(cs3, cr2)
            print 'Secondary Color - ' + cs3
            # Replace for the secondary font color
            cp4 = re.compile(r"#faffff")
            cr4 = cp4.sub(cs4, cr3)
            print 'Secondary Font - ' + cs4

            # Replace header border color and check for lightness
            # Convert from hex to hsv
            cs1_r, cs1_g, cs1_b = hex_to_rgb(cs1)
            cs1_h, cs1_s, cs1_v = rgb_to_hsv(cs1_r, cs1_g, cs1_b)
            cp5 = re.compile(r"#002142")
            # Check to see if header is light enough to need a border
            if cs1_s <= .2 and cs1_v >= .8:
                cs5 = '#444444'
                cr5 = cp5.sub(cs5, cr4)
                print 'Header too light, adding border - ' + cs5
            else:
                cr5 = cp5.sub(cs1, cr4)
                print 'Header Line removed'

            # write the file
            fo = open(new_file, "wb")
            fo.write(cr5)
            fo.close()
            print new_file + " has been customized"
        else:
            # write the file
            fo = open(new_file, "wb")
            fo.write(bo)
            fo.close()
            print new_file + " uses the standard color scheme"

        # Repoint the newly created sanbox in the adv_id file
        for name in glob.glob(adv_id + '/*Sandbox*.twb'):
            base_name = os.path.basename(name)
            file_name, file_ext = os.path.splitext(base_name)
            sb_rp = sandbox_repointer(name)
            sb_rp.repoint(adv_id, adv_name)
            print adv_id + ' Sandbox.twb repointed'

        # rebuilds the twbx after the update with new image folder
        # only pulls twbs with 'Packaged' at the end of the file name
        # adv_id version
        for name in glob.glob(adv_id + '/*Packaged.twb'):
            base_name = os.path.basename(name)
            file_name, file_ext = os.path.splitext(base_name)
            twbx_temp = adv_id + '/' + file_name + '.temp'
            os.makedirs(twbx_temp)
            os.makedirs(twbx_temp + '/Image')
            shutil.move(name, twbx_temp)
            # copies Image folder to temp directory
            image_base = adv_id + '/Image'
            for item in os.listdir(image_base):
                s = os.path.join(image_base, item)
                d = os.path.join(twbx_temp + '/Image', item)
                if os.path.isdir(s):
                    shutil.copytree(s, d, symlinks=False)
                else:
                    shutil.copy2(s, d)
            os.makedirs(twbx_temp + '/Data')
            # copies Data folder to temp directory
            data_base = adv_id + '/Data'
            for item in os.listdir(data_base):
                s = os.path.join(data_base, item)
                d = os.path.join(twbx_temp + '/Data', item)
                if os.path.isdir(s):
                    shutil.copytree(s, d, symlinks=False)
                else:
                    shutil.copy2(s, d)

        # look for folders with .temp zip it
        for name in glob.glob(adv_id + '/*temp'):
            base_name = os.path.basename(name)
            file_name, file_ext = os.path.splitext(base_name)
            shutil.make_archive(adv_id + '/' + file_name, 'zip', name)
            shutil.rmtree(name)

        # change the .zip extension to .twbx
        for name in glob.glob(adv_id + '/*.zip'):
            base_name = os.path.basename(name)
            file_name, file_ext = os.path.splitext(base_name)
            print file_name + '.twbx'
            if os.path.exists(adv_id + '/' + file_name + '.twbx'):
                os.remove(adv_id + '/' + file_name + '.twbx')
                print 'Removed old version of ' + new_file
            os.rename(name, adv_id + '/' + file_name + '.twbx')

    # Remove extra folders
    shutil.rmtree(adv_id + '/Image')
    shutil.rmtree(adv_id + '/Data')


def getsbid(advertiser_name):
    # Checks to see if there is a sandbox already on the site
    # Where are we connecting
    conn_string = "host='p-p3tableaum01-1a.use01.plat.priv' dbname='workgroup' port='8060' user='readonly' password='dDCAJP9pSpfw'"

    # print the connection string we will use to connect
    print "Connecting to database..."

    # get a connection, if a connect cannot be made an exception will be
    # raised here
    conn = psycopg2.connect(conn_string)

    # conn.cursor will return a cursor object, you can use this cursor to
    # perform the desired query
    cursor = conn.cursor()

    # execute Query
    cursor.execute("SELECT p.id\
                    FROM _sites s\
                    LEFT JOIN _projects p ON p.site_id = s.id\
                    LEFT JOIN _workbooks w ON w.site_id = s.id\
                    WHERE w.name is not null and s.url_namespace = %s and p.name = 'Sandbox'\
                    GROUP BY p.id", (advertiser_name,))

    # retrieve the records from the database
    rows = cursor.fetchone()
    id = rows[0]
    print 'Workbook -> Sandbox'
    print 'ID -> ' + str(id)

    return id


class sandbox_repointer(object):
    # Redirects the sandbox to the new adv_name site

    def __init__(self, sandbox_file):
        self.sandbox_file = sandbox_file

    def repoint(self, advertiser_id, advertiser_name):
        '''Takes an advertiser_name and repoints the sandbox to their site'''

        with open(self.sandbox_file, 'rb+') as sandbox:
            data = sandbox.read()  # Open the file and read it to memory

            # finds the path for the advetiser site nad replaces it in the two
            # places
            new_data = re.sub(
                r"path='/t/\w+/", "path='/t/%s/" % advertiser_name, data)
            new_data = re.sub(r"site='\w+'", "site='%s'" %
                              advertiser_name, new_data)

            sandbox.seek(0)  # go to the beggning of the file
            sandbox.write(new_data)  # write new data
            sandbox.truncate()  # eliminate the old data
            sandbox.close()  # close the file


def runTabCmd(cmd):
    # run tableau command and display the output
    # may need to parse the returned information
    print cmd

    if run_tabcmd == 'yes':
        p = subprocess.Popen(
            cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        for line in p.stdout.readlines():
            print line,
        # retval = p.wait()

        # is this necessary?
        time.sleep(2)


def doPublish(adv_dir, adv_name, update_sandbox):
    # publish datasources first
    for ds in tdss:
        try:
            sandbox_id = getsbid(adv_name)
            if sandbox_id is None:
                runTabCmd('tabcmd createproject -n Sandbox')
            else:
                print 'Sandbox project id', sandbox_id
        except:
            runTabCmd('tabcmd createproject -n Sandbox')
        ds = os.path.join(adv_dir, ds)
        runTabCmd('tabcmd publish ' + ds + ' --db-username ' + db_user +
                  ' --db-password ' + db_pw + cmd_opt + ' --project "Sandbox"')

    # publish to workbook and data source
    for wb in twbs:
        wb_split = wb.split('_')

        # check the length of the file
        if len(wb_split) == 5 and re.search('Packaged', wb) and re.search('Sandbox', wb) is None:
            wb = wb + 'x'
            wb = os.path.join(adv_dir, wb)
            print wb
            # if it's the proper length grab index 2, should be the name
            wb_name = wb_split[2].split('.')[0]
            runTabCmd('tabcmd publish ' + wb + ' -n ' + wb_name +
                      ' --db-username ' + db_user + ' --db-password ' + db_pw + cmd_opt + ' --tabbed')
        elif len(wb_split) == 5 and re.search('Sandbox', wb):
            if update_sandbox == 'no':
                print 'Checking for Sandbox'
                # Check to see if there is already a Sanbox on the site
                try:
                    getsbid(adv_name)
                    print 'Sandbox exists on site - Waiting 30 sec'
                except:
                    # If getsbid() returns empty, upload the newly repointed
                    # version
                    print 'No Sandbox on site'

                    wb = wb + 'x'
                    # wb = wb
                    wb = os.path.join(adv_dir, wb)
                    print wb
                    # if it's the proper length grab index 2, should be the
                    # name
                    wb_name = wb_split[2].split('.')[0]
                    runTabCmd('tabcmd publish ' + wb + ' -n ' + wb_name +
                              ' --db-username ' + db_user + ' --db-password ' + db_pw + cmd_opt + ' --project "Sandbox"')
            elif update_sandbox == 'yes':
                print 'Overwriting existing Sandbox'
                wb = wb + 'x'
                # wb = wb
                wb = os.path.join(adv_dir, wb)
                print wb
                # if it's the proper length grab index 2, should be the name
                wb_name = wb_split[2].split('.')[0]
                runTabCmd('tabcmd publish ' + wb + ' -n ' + wb_name +
                          ' --db-username ' + db_user + ' --db-password ' + db_pw + cmd_opt + ' --project "Sandbox"')

        # check the le  ngth of the file
        elif len(wb_split) == 5 and re.search('Packaged', wb) is None:
            # if it's the proper length grab index 2, should be the name
            wb_name = wb_split[2].split('.')[0]
            wb = os.path.join(adv_dir, wb)
            print wb
            runTabCmd('tabcmd publish ' + wb + ' -n ' + wb_name +
                      ' --db-username ' + db_user + ' --db-password ' + db_pw + cmd_opt)

        else:
            wb_name = 'Workbook'
            wb = os.path.join(adv_dir, wb)
            runTabCmd('tabcmd publish ' + wb + ' -n ' + wb_name +
                      ' --db-username ' + db_user + ' --db-password ' + db_pw + cmd_opt)

        time.sleep(30)  # add 30 sec to let tableau server breathe


def printUsage(name):
    # display a usage hint
    print name, '[--advertiser_id=<id> --advertiser_name=<name> [--sharded=<yes/no>] [--from_port=<port> --to_port=<port>] [--config=<file>] [--run_tabcmd=<yes/no>] [--update_sandbox=<yes/no>]]'
    print name, 'version', version


def main(argv):
    parms = len(sys.argv)
    advertiser_name = '0000'
    advertiser_id = '0000'
    from_port = '0000'
    to_port = '0000'
    project = '0000'
    sharded = 'no'
    config = 'advertiser.config'
    update_sandbox = 'no'
    global run_tabcmd

    try:
        opts, args = getopt.getopt(argv, "h", [
                                   "advertiser_id=", "advertiser_name=", "sharded=", "from_port=", "to_port=", "project=", "config=", "run_tabcmd=", 'update_sandbox='])
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
        elif opt in ("--sharded"):
            sharded = arg.lower()
        elif opt in ("--from_port"):
            from_port = arg
        elif opt in ("--to_port"):
            to_port = arg
        elif opt in ("--project"):
            project = arg
        elif opt in ("--config"):
            config = arg
            parms -= 1
        elif opt in ("--run_tabcmd"):
            run_tabcmd = arg.lower()
            parms -= 1
        elif opt in ("--update_sandbox"):
            update_sandbox = arg.lower()
            parms -= 1

    # cover sharded=y/Y
    if sharded == "y":
        sharded = "yes"

    # cover run_tabcmd=y/Y
    if run_tabcmd == "y":
        run_tabcmd = "yes"

    # cover update_sandbox=y/Y
    if update_sandbox == "y":
        update_sandbox = "yes"

    # print out parameters
    print 'advertiser_id ', advertiser_id
    print 'advertiser_name ', advertiser_name
    print 'from_port ', from_port
    print 'to_port ', to_port
    print 'project', project
    print 'sharded ', sharded
    print 'config ', config
    print 'run_tabcmd ', run_tabcmd
    print 'update_sandbox', update_sandbox

    # a little validation
    if parms > 1:
        if advertiser_id != '0000' and advertiser_name == '0000':
            raise NameError("Invalid customer name -- %s" % advertiser_name)

    # read in the configuration
    getConfiguration(config)

    # process the command
    # new code added to unzip the packaged workbooks before upload
    if advertiser_id != '3444':
        for name in glob.glob('3444/*.twbx'):
            file_name, file_ext = os.path.splitext(name)
            os.rename(name, file_name + '.zip')
        for name in glob.glob('3444/*.zip'):
            zip = zipfile.ZipFile(name, 'r')
            zip.extractall('3444')

    if advertiser_id != '0000':
        singleAdvertiser(
            advertiser_id, advertiser_name, from_port, to_port, sharded, update_sandbox)
    else:
        allAdvertisers(from_port, to_port)

if __name__ == "__main__":
    main(sys.argv[1:])
