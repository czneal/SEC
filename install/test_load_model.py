from tensorflow.keras.models import load_model
try:
   print('load new format model ...', end='')
   m = load_model('model.h5')
   print('ok')
except Exception as e:
   print('fail')
   print(e)

try:
   print('load old format model ...', end='')
   m = load_model('model_old.h5')
   print('ok')
except Exception as e:
   print('fail')
   print(e)