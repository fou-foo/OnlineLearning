from django.http import HttpResponse

# Create your views here.

def index(request) :
    resp = HttpResponse()
    resp.set_cookie('dj4e_cookie', 'd6b6536a', max_age=1000) # seconds until expire

    num_visits = request.session.get('num_visits', 0) + 1
    request.session['num_visits'] = num_visits 
    if num_visits > 4 : del(request.session['num_visits'])
    return HttpResponse('view count='+str(num_visits))

def cookie(request):
    print(request.COOKIES)
    oldval = request.COOKIES.get('dj4e_cookie', None)
    resp = HttpResponse('In a view - the zap cookie value is '+str(oldval))
    if oldval : 
        resp.set_cookie('dj4e_cookie', 'd6b6536a') # No expired date = until browser close
    else : 
        resp.set_cookie('dj4e_cookie', 'd6b6536a') # No expired date = until browser close
    resp.set_cookie('dj4e_cookie', 'd6b6536a', max_age=1000) # seconds until expire
    return resp

