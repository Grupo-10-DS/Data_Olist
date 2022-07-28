echo "Enter your message"
read message
python delta.py
git pull
git add .
git commit -m"${message}"
git status
git push origin main
