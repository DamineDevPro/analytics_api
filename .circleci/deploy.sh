ssh -o StrictHostKeyChecking=no ${uname}@${ip} << EOF
echo "1. pull code from git ......"
cd ${path}
echo pwd
sudo su
chmod -R 777 ${path}
git reset --hard
git clean -fd
git checkout --force development
git pull
echo "2. Installing Dependency......"
python3 -m venv venv
source ./venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
echo ".Copy Dependency......"
cp com.github.jnr_jffi-1.2.19.jar /root/.ivy2/jars/com.github.jnr_jffi-1.2.19.jar
cp org.codehaus.groovy_groovy-2.5.7.jar /root/.ivy2/jars/org.codehaus.groovy_groovy-2.5.7.jar
cp org.codehaus.groovy_groovy-json-2.5.7.jar /root/.ivy2/jars/org.codehaus.groovy_groovy-json-2.5.7.jar

cp com.github.jnr_jffi-1.2.19.jar /home/ubuntu/.ivy2/jars/com.github.jnr_jffi-1.2.19.jar
cp org.codehaus.groovy_groovy-2.5.7.jar /home/ubuntu/.ivy2/jars/org.codehaus.groovy_groovy-2.5.7.jar
cp org.codehaus.groovy_groovy-json-2.5.7.jar /home/ubuntu/.ivy2/jars/org.codehaus.groovy_groovy-json-2.5.7.jar

echo "3. Restart pm2 server......"
pm2 restart ecosystem.config.js
echo '----------------------------------Done!----------------------------------'
EOF