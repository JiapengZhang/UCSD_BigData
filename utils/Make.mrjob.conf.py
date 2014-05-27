<<<<<<< HEAD
" Insert credentials into mrjob configuration file "
import sys, os, pickle

=======
""" Insert credentials into mrjob configuration file """
import sys, os, pickle
                    
>>>>>>> d1f988374d83195d0a7bbbcbd840c8592ae68e3a
if 'EC2_VAULT' in os.environ.keys():
    vault=os.environ['EC2_VAULT']
else:  # If EC2_VAULT is not defined, we assume we are in an EC2 instance
    vault='/home/ubuntu/Vault'
try:
<<<<<<< HEAD
    with open(vault+'/Creds.pkl') as file:
        Creds=pickle.load(file)
    #print 'Creds=',Creds
    keypair=Creds['mrjob']
    print 'keypair=',keypair
    template=open('mrjob.conf.template').read()
    filled= template % ('giz',keypair['key_id'],keypair['secret_key'])
=======
    vaultname=vault+'/Creds.pkl'
    def check(key,Dict ):
        if not key in Dict.keys():
            sys.exit('The file: '+vaultname+' Does not contain the key "'+\
                     key+'" in the correct place"')

    with open(vaultname) as file:
        Creds=pickle.load(file)

    check('mrjob',Creds)
    keypair=Creds['mrjob']
    template=open('mrjob.conf.template').read()

    check('key_id',keypair)
    check('secret_key',keypair)
    filled= template % (keypair['key_id'],keypair['secret_key'])
    # print 'filled=\n',filled
>>>>>>> d1f988374d83195d0a7bbbcbd840c8592ae68e3a
    home=os.environ['HOME']
    outfile = home+'/.mrjob.conf'
    open(outfile,'wb').write(filled)
    print 'Created the configuration file:',outfile
except Exception, e:
    print e

<<<<<<< HEAD
=======

>>>>>>> d1f988374d83195d0a7bbbcbd840c8592ae68e3a
