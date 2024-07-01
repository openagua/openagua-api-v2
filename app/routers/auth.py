import datetime as dt
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Response, Request, Form
from fastapi.security import OAuth2PasswordRequestForm

from pydantic import validate_email, BaseModel, HttpUrl, EmailStr

from app.deps import get_db, authorized_user
from app.schemas import User
from app.core.users import get_user_by_email, get_datauser, register_datauser
from app.core.security import confirm_email_token_status, confirm_user, reset_password_token_status, \
    send_reset_password_instructions, update_password, register_user
from app.core.utils import verify_password, create_access_token, create_refresh_token, decode_refresh_token, \
    send_confirmation_instructions

LoginException = HTTPException(status_code=401, detail="Incorrect username or password")


# class RegistrationRequestForm(OAuth2PasswordRequestForm):
#     origin: HttpUrl


class PasswordResetRequest(BaseModel):
    origin: HttpUrl
    email: EmailStr


class PasswordReset(BaseModel):
    password: str
    token: str


api = APIRouter(prefix='/auth', tags=['Authorization'])


@api.post('/login')
async def _login(response: Response, form_data: OAuth2PasswordRequestForm = Depends(), db=Depends(get_db)):
    user = get_user_by_email(db, form_data.username)
    if not user:
        raise LoginException
    if not verify_password(form_data.password, user.password):
        raise LoginException
    access_token = create_access_token(user.email)
    refresh_token = create_refresh_token(user.email)
    response.set_cookie(key="access_token", value=access_token, httponly=True)
    response.set_cookie(key="refresh_token", value=refresh_token, httponly=True)
    return user.to_json(include_id=True)


@api.post('/logout')
async def _logout(response: Response):
    expires = dt.datetime.utcnow() + dt.timedelta(seconds=1)
    expires_str = expires.strftime("%a, %d %b %Y %H:%M:%S GMT")
    for token in ['access', 'refresh']:
        response.set_cookie(
            key=f'{token}_token',
            value="",
            secure=True,
            httponly=True,
            # samesite='none',
            expires=expires_str,
            # domain="example.com"
        )


@api.post('/verify_token')
async def _verify_token(user=Depends(authorized_user)) -> User:
    return user.to_json(include_id=True)


@api.get('/refresh_token')
async def _refresh_token(request: Request, response: Response, db=Depends(get_db)) -> User:
    try:
        refresh_token = request.cookies.get('refresh_token')
        payload = decode_refresh_token(refresh_token)
        user = get_user_by_email(db, payload['sub'])
        access_token = create_access_token(user.email)
        refresh_token = create_refresh_token(user.email)
        response.set_cookie(key="access_token", value=access_token, secure=True, httponly=True)
        response.set_cookie(key="refresh_token", value=refresh_token, secure=True, httponly=True)
        return user.to_json(include_id=True)
    except:
        for token in ['access_token', 'refresh_token']:
            response.delete_cookie(token)
        raise HTTPException(401)


@api.post('/register')
async def _register(username: Annotated[EmailStr, Form()], password: Annotated[str, Form()],
                    origin: Annotated[HttpUrl, Form()],
                    db=Depends(get_db)):
    email = username

    try:
        validate_email(email)
    except:
        raise HTTPException(400, 'Invalid email format')

    if get_user_by_email(db, email):
        raise HTTPException(409, 'Already registered')

    user = await register_user(db, email=username, password=password, origin=origin)

    return user.to_json()


@api.post('/confirm_registration')
async def _confirm_registration(origin: str, token: str, db=Depends(get_db)):
    expired, invalid, user = confirm_email_token_status(db, token)

    if not user or invalid:
        invalid = True

    already_confirmed = user is not None and user.confirmed_at is not None

    if expired and not already_confirmed:
        await send_confirmation_instructions(origin, user)

    if invalid or (expired and not already_confirmed):
        raise HTTPException(401, 'Invalid or expired')

    if confirm_user(db, user):
        return Response(200)
    else:
        raise HTTPException(401, 'Already confirmed')


@api.post('/reset_password_request', status_code=200)
async def _create_forgot_password_token(data: PasswordResetRequest, db=Depends(get_db)) -> None:
    origin = data.origin
    email = data.email
    await send_reset_password_instructions(db, origin, email)


@api.get('/verify_forgot_password_token', status_code=200)
async def _verify_forgot_password_token(token: str, db=Depends(get_db)):
    try:
        expired, invalid, user = reset_password_token_status(db, token)
        if expired:
            return HTTPException(401, 'Token expired')
        elif invalid:
            return HTTPException(401, 'Invalid token')
    except:
        raise HTTPException(401, 'Invalid token')

    return


@api.post('/reset_password', status_code=200)
async def _reset_password(data: PasswordReset, db=Depends(get_db)):
    token = data.token
    password = data.password
    expired, invalid, user = reset_password_token_status(db, token)

    if invalid:
        raise HTTPException(401, 'Invalid token')
    if expired:
        # send_reset_password_instructions(db, data.origin, user)
        raise HTTPException(401, 'Token expired')

    datauser = get_datauser(db, user_id=user.id)
    if not datauser:
        datauser = register_datauser(
            db,
            user_id=user.id,
            username=user.email,
            password=password  # This updates the password on Hydra, not OpenAgua
        )

    await update_password(db, user.id, datauser.userid, password)
