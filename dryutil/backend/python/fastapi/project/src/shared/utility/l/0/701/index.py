from typing import Any
from fastapi import Request
from fastapi.responses import JSONResponse, PlainTextResponse
from sqlalchemy import Column, String, Text, DateTime
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.orm import declarative_base
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.future import select
import uuid
import httpx
from datetime import datetime, timezone
from src.shared.utility.u.fake_req_obj.index import fake_req_obj


Base = declarative_base()

class WhatsappBusiness(Base):
    __tablename__ = "whatsapp_business"
    id      = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id = Column(String(155), nullable=False)
    data    = Column(JSONB, nullable=True)

class WhatsappLog(Base):
    __tablename__ = "whatsapp_log"
    id          = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    business_id = Column(String(155), nullable=True)
    direction   = Column(String(10), nullable=False)   # 'in' | 'out' | 'event'
    from_       = Column("from", String(50), nullable=True)
    to_         = Column("to", String(50), nullable=True)
    message     = Column(Text, nullable=True)
    msg_status  = Column(String(20), nullable=True, default="sent")  # sent/delivered/read
    wa_msg_id   = Column(String(100), nullable=True)   # Meta message id for status tracking
    payload     = Column(JSONB, nullable=True)
    created_at  = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))


async def index(_p={'data': {'instance': Any}}):

    # -- db setup --
    _engine = None
    if _p['data'].get('instance') is not None:
        _data        = _p['data']['instance'].data
        _config      = _data.get('config', {}) \
            if isinstance(_p['data']['instance'].data, dict) else {}
        database_url = _config.get('database', {}).get('url')
        if database_url:
            _engine = create_async_engine(database_url, echo=False, future=True)
            async with _engine.begin() as conn:
                await conn.run_sync(Base.metadata.create_all)

    def _err(msg, log):
        return JSONResponse(content={"success": False, "message": msg, "data": {"log": log}}, status_code=404)

    def _meta_cfg(row):
        d = row.data or {}
        return d.get('config', {}).get('meta', {})

    async def _get_row(db, body):
        if 'id' in body:
            result = await db.execute(
                select(WhatsappBusiness).where(WhatsappBusiness.id == uuid.UUID(body['id']))
            )
            return result.scalar_one_or_none()
        result = await db.execute(
            select(WhatsappBusiness).where(WhatsappBusiness.user_id == body['user_id'])
        )
        return result.scalars().first()

    async def _get_row_by_user(db, user_id):
        result = await db.execute(
            select(WhatsappBusiness).where(WhatsappBusiness.user_id == user_id)
        )
        return result.scalars().first()

    async def _get_first_business_row(db):
        result = await db.execute(select(WhatsappBusiness).limit(1))
        return result.scalar_one_or_none()

    # ── Meta Graph API helpers ──────────────────────────────────────────────

    async def _push_profile_to_meta(token: str, phone_id: str, profile: dict):
        """Push business profile fields to Meta WhatsApp Business Profile API"""
        payload = {"messaging_product": "whatsapp"}
        if profile.get('description'):
            payload['about'] = profile['description']
        if profile.get('logo_url'):
            payload['profile_picture_url'] = profile['logo_url']
        if profile.get('category'):
            payload['vertical'] = profile['category']
        async with httpx.AsyncClient() as client:
            r = await client.post(
                f"https://graph.facebook.com/v18.0/{phone_id}/whatsapp_business_profile",
                headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
                json=payload,
                timeout=15
            )
        return r.status_code, r.json()

    async def _subscribe_webhook(token: str, waba_id: str, fields: list = None):
        """Subscribe WABA to webhook fields"""
        payload = {}
        if fields:
            payload['subscribed_fields'] = fields
        async with httpx.AsyncClient() as client:
            r = await client.post(
                f"https://graph.facebook.com/v18.0/{waba_id}/subscribed_apps",
                headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
                json=payload,
                timeout=15
            )
        return r.status_code, r.json()

    async def _push_product_to_meta(client: httpx.AsyncClient, token: str, catalog_id: str, product: dict):
        """Push single product to Meta catalog"""
        price_raw = product.get('price', 0)
        try:
            price_cents = int(float(str(price_raw).replace(',', '').strip()) * 100)
        except Exception:
            price_cents = 0
        payload = {
            "retailer_id":   str(product.get('id', product.get('slug', ''))),
            "name":          product.get('name', product.get('title', '')),
            "description":   product.get('description', product.get('name', '')),
            "price":         price_cents,
            "currency":      product.get('currency', 'INR'),
            "availability":  "in stock",
            "url":           product.get('url', product.get('link', '')),
            "image_url":     product.get('image', product.get('image_url', '')),
        }
        r = await client.post(
            f"https://graph.facebook.com/v18.0/{catalog_id}/products",
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
            json=payload,
            timeout=15
        )
        return r.status_code, r.json()

    async def _send_whatsapp_message(token: str, phone_id: str, to: str, payload: dict):
        async with httpx.AsyncClient() as client:
            r = await client.post(
                f"https://graph.facebook.com/v25.0/{phone_id}/messages",
                headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
                json={"messaging_product": "whatsapp", "to": to, **payload},
                timeout=15
            )
        return r.json()

    async def _send_text(token: str, phone_id: str, to: str, text: str):
        return await _send_whatsapp_message(token, phone_id, to, {
            "type": "text", "text": {"body": text}
        })

    async def _send_product_list(token: str, phone_id: str, to: str, products: list, title: str = "Our Products"):
        rows = []
        for p in products[:10]:
            doc   = p.get('document', p)  # typesense format
            if not rows:
                print(f"[product_keys] {list(doc.keys())} price={doc.get('price')} mrp={doc.get('mrp')} selling_price={doc.get('selling_price')}")
            name  = str(doc.get('name', doc.get('title', 'Product')))[:24]
            # price is in variant_mrp or variant_prices
            variant_mrp = doc.get('variant_mrp', [])
            variant_prices = doc.get('variant_prices', [])
            price = ''
            if variant_mrp and isinstance(variant_mrp, list) and len(variant_mrp) > 0:
                price = variant_mrp[0]
            elif variant_prices and isinstance(variant_prices, list) and len(variant_prices) > 0:
                price = variant_prices[0]
            desc  = f"\u20b9{price}" if price else ""
            rows.append({"id": str(doc.get('id', doc.get('slug', uuid.uuid4())))[:200], "title": name, "description": desc[:72]})
        if not rows:
            return await _send_text(token, phone_id, to, "No products found.")
        return await _send_whatsapp_message(token, phone_id, to, {
            "type": "interactive",
            "interactive": {
                "type": "list",
                "header": {"type": "text", "text": title[:60]},
                "body":   {"text": "Tap to view products"},
                "footer": {"text": "Reply with a product name to learn more"},
                "action": {"button": "View Products", "sections": [{"title": title[:24], "rows": rows}]}
            }
        })

    # ── Product dir helper ──────────────────────────────────────────────────

    async def _fetch_products(token: str, base_url: str, q: str = "*", per_page: int = 250):
        print(f"[fetch_products] calling base_url={base_url}")
        async with httpx.AsyncClient() as client:
            resp = await client.post(
                f"{base_url}/client/api/i/ona/product_dir?typ=get_product_list",
                headers={"Authorization": f"Bearer {token}"},
                json={"q": q, "per_page": per_page, "page": 1},
                timeout=30
            )
        print(f"[fetch_products] status={resp.status_code} body={resp.text[:300]}")
        return resp.json()

    # ── Log helper ──────────────────────────────────────────────────────────

    async def _log(db, direction: str, from_: str, to_: str, message: str, payload: dict,
                   business_id: str = None, wa_msg_id: str = None):
        log = WhatsappLog(
            business_id=business_id,
            direction=direction,
            from_=from_,
            to_=to_,
            message=message,
            payload=payload,
            wa_msg_id=wa_msg_id,
            msg_status="sent" if direction == "out" else None,
        )
        db.add(log)
        await db.flush()

    # ── Template variable substitution ─────────────────────────────────────

    def _apply_template(template: str, variables: dict) -> str:
        result = template
        for k, v in variables.items():
            result = result.replace(f"{{{{{k}}}}}", str(v))
        return result

    # ── Conversation engine ─────────────────────────────────────────────────

    async def _is_new_customer(db, phone: str) -> bool:
        result = await db.execute(
            select(WhatsappLog).where(WhatsappLog.from_ == phone).limit(1)
        )
        return result.scalar_one_or_none() is None

    async def _handle_incoming_message(db, row, from_number: str, msg_text: str, msg_id: str):
        # read fresh config using a NEW session to bypass SQLAlchemy cache
        async with AsyncSession(_engine) as fresh_session:
            from sqlalchemy import text as sa_text
            result = await fresh_session.execute(
                sa_text("SELECT data FROM whatsapp_business WHERE id = :id"),
                {"id": str(row.id)}
            )
            row_data = result.fetchone()
            data = dict(row_data[0]) if row_data and row_data[0] else (row.data or {})

        cfg      = data.get('config', {})
        meta     = cfg.get('meta', {})
        token    = meta.get('access_token')
        phone_id = meta.get('phone_number_id')
        base_url = cfg.get('base_url', 'http://localhost:8000')
        # force remote server — localhost product_dir not available from webhook
        if base_url == 'http://localhost:8000':
            base_url = 'https://fastapi.dryutil.1mn.io'
        pd_token = cfg.get('product_dir_token', '')
        print(f"[handle] from={from_number} text={msg_text} token_set={bool(token)} base_url={base_url} pd_token_set={bool(pd_token)}")
        if not token or not phone_id:
            print(f"[handle] MISSING credentials, skipping")
            return

        is_new = await _is_new_customer(db, from_number)

        # log incoming
        await _log(db, 'in', from_number, phone_id, msg_text, {"msg_id": msg_id}, str(row.id), msg_id)

        automation = data.get('automation', {})
        toggles    = automation.get('toggles', {})

        if toggles.get('auto_reply') is False:
            await db.commit()
            return

        store_name        = data.get('profile', {}).get('title', data.get('title', 'Our Store'))
        welcome_msg       = automation.get('welcome_message') or \
            f"Welcome to *{store_name}*! \U0001f44b\n\nHow can I help you?\n\n1\ufe0f\u20e3 *products* - Browse products\n2\ufe0f\u20e3 *categories* - View categories\n3\ufe0f\u20e3 *help* - Contact seller"
        default_reply_msg = automation.get('default_reply') or "Type *menu* to see options."
        product_template  = automation.get('product_template') or "Available Products"
        category_template = automation.get('category_template') or "Product Categories"

        text = msg_text.strip().lower()

        # handle product selection from interactive list
        if msg_text.startswith('product:'):
            product_name = msg_text[8:].strip()
            try:
                pd_resp  = await _fetch_products(pd_token, base_url)
                all_products = pd_resp.get('data', {}).get('products', [])
                if not all_products:
                    all_products = pd_resp.get('data', {}).get('hits', [])
                # filter by product name - match any word
                search_words = product_name.lower().split()
                def matches(p):
                    doc = p.get('document', p)
                    name = str(doc.get('name', '')).lower()
                    cat  = ' '.join([str(c) for c in doc.get('category', [])]).lower()
                    combined = name + ' ' + cat
                    return any(w in combined for w in search_words)
                filtered = [p for p in all_products if matches(p)]
                products_to_show = filtered if filtered else all_products
                print(f"[product_select] name={product_name} filtered={len(filtered)} total={len(all_products)}")
                if products_to_show:
                    # send as plain text message in chat
                    reply = f"*{product_name} Products:*\n\n"
                    for p in products_to_show[:10]:
                        doc = p.get('document', p)
                        name = doc.get('name', doc.get('title', ''))
                        variant_mrp = doc.get('variant_mrp', [])
                        variant_prices = doc.get('variant_prices', [])
                        price = ''
                        if variant_mrp and isinstance(variant_mrp, list) and len(variant_mrp) > 0:
                            price = variant_mrp[0]
                        elif variant_prices and isinstance(variant_prices, list) and len(variant_prices) > 0:
                            price = variant_prices[0]
                        price_str = f" - \u20b9{price}" if price else ""
                        reply += f"\U0001f455 *{name}*{price_str}\n"
                    reply += "\nType *products* to browse more."
                    r = await _send_text(token, phone_id, from_number, reply)
                else:
                    r = await _send_text(token, phone_id, from_number, f"No products found for '{product_name}'.\nType *products* to browse all.")
                await _log(db, 'out', phone_id, from_number, f"product detail: {product_name}", r, str(row.id))
            except Exception as e:
                print(f"[product_select] ERROR: {e}")
                r = await _send_text(token, phone_id, from_number, f"Type *products* to browse all products.")
                await _log(db, 'out', phone_id, from_number, product_name, {}, str(row.id))
            await db.commit()
            return
        if is_new:
            r = await _send_text(token, phone_id, from_number, welcome_msg)
            await _log(db, 'out', phone_id, from_number, welcome_msg, r, str(row.id),
                       r.get('messages', [{}])[0].get('id') if isinstance(r, dict) else None)
            await db.commit()
            return

        if text in ('hi', 'hello', 'hey', 'start', 'menu'):
            r = await _send_text(token, phone_id, from_number, welcome_msg)
            await _log(db, 'out', phone_id, from_number, welcome_msg, r, str(row.id))

        elif text in ('products', 'catalog', '1', 'shop', 'browse'):
            try:
                pd_resp  = await _fetch_products(pd_token, base_url)
                products = pd_resp.get('data', {}).get('products', pd_resp.get('data', {}).get('hits', []))
                if isinstance(products, dict):
                    products = products.get('hits', products.get('products', []))
                print(f"[products] count={len(products)} sample={str(products[0])[:100] if products else 'empty'}")
                r = await _send_product_list(token, phone_id, from_number, products, product_template)
                print(f"[products] send result={r}")
                await _log(db, 'out', phone_id, from_number, f"Sent {len(products)} products", r, str(row.id))
            except Exception as e:
                import traceback
                print(f"[products] ERROR: {e}\n{traceback.format_exc()}")
                err_text = "Sorry, couldn't load products right now."
                r = await _send_text(token, phone_id, from_number, err_text)
                await _log(db, 'out', phone_id, from_number, err_text, {"error": str(e)}, str(row.id))

        elif text in ('categories', 'category', '2'):
            try:
                pd_resp  = await _fetch_products(pd_token, base_url)
                products = pd_resp.get('data', {}).get('hits', [])
                if isinstance(products, dict):
                    products = products.get('hits', [])
                cats = list({p.get('document', p).get('category', '') for p in products
                             if p.get('document', p).get('category')})
                if cats:
                    cat_text = _apply_template(category_template, {
                        "category_name": ", ".join(cats[:5]),
                        "product_count": len(products),
                        "product_list":  "\n".join(f"• {c}" for c in cats[:15])
                    }) if "{{" in category_template else \
                        f"*{category_template}:*\n" + "\n".join(f"• {c}" for c in cats[:15]) + \
                        "\n\nReply with a category name to see products."
                else:
                    cat_text = "No categories found. Type *products* to browse all."
                r = await _send_text(token, phone_id, from_number, cat_text)
                await _log(db, 'out', phone_id, from_number, cat_text, r, str(row.id))
            except Exception as e:
                r = await _send_text(token, phone_id, from_number, "Couldn't load categories right now.")
                await _log(db, 'out', phone_id, from_number, "error", {"error": str(e)}, str(row.id))

        elif text in ('help', 'contact', '3', 'support'):
            help_text = f"Need help? Contact us:\n\n🏪 *{store_name}*\nWe'll get back to you shortly!\n\nType *menu* to go back."
            r = await _send_text(token, phone_id, from_number, help_text)
            await _log(db, 'out', phone_id, from_number, help_text, r, str(row.id))

        elif toggles.get('product_search') is not False:
            try:
                pd_resp  = await _fetch_products(pd_token, base_url, q=msg_text.strip(), per_page=5)
                products = pd_resp.get('data', {}).get('hits', [])
                if isinstance(products, dict):
                    products = products.get('hits', [])
                if products:
                    doc = products[0].get('document', products[0])
                    if "{{" in product_template:
                        reply = _apply_template(product_template, {
                            "product_name": doc.get('name', ''),
                            "price":        doc.get('price', ''),
                            "description":  doc.get('description', ''),
                            "image_url":    doc.get('image', doc.get('image_url', '')),
                            "availability": "In Stock",
                        })
                        r = await _send_text(token, phone_id, from_number, reply)
                        await _log(db, 'out', phone_id, from_number, reply, r, str(row.id))
                    else:
                        r = await _send_product_list(token, phone_id, from_number, products,
                                                     f"Results for '{msg_text.strip()}'")
                        await _log(db, 'out', phone_id, from_number, f"Search: {msg_text}", r, str(row.id))
                else:
                    r = await _send_text(token, phone_id, from_number, default_reply_msg)
                    await _log(db, 'out', phone_id, from_number, default_reply_msg, r, str(row.id))
            except Exception:
                r = await _send_text(token, phone_id, from_number, default_reply_msg)
                await _log(db, 'out', phone_id, from_number, default_reply_msg, {}, str(row.id))
        else:
            r = await _send_text(token, phone_id, from_number, default_reply_msg)
            await _log(db, 'out', phone_id, from_number, default_reply_msg, r, str(row.id))

        await db.commit()

    def _to_catalog_item(product: dict):
        return {
            "id":           str(product.get("id", product.get("slug", ""))),
            "availability": "in stock",
            "condition":    "new",
            "description":  product.get("description", product.get("name", "")),
            "image_link":   product.get("image", product.get("image_url", "")),
            "link":         product.get("url", product.get("link", "")),
            "title":        product.get("name", product.get("title", "")),
            "price":        f"{product.get('price', 0)} {product.get('currency', 'INR')}",
            "retailer_id":  str(product.get("id", product.get("slug", ""))),
        }


    # ---- i ----
    async def i(request: Request):
        try:
            if _engine is None:
                raise Exception("database engine not configured — ensure instance data.config.database.url is set")
            body = await request.json()
            typ  = request.query_params.get('typ')

            async with AsyncSession(_engine) as db:
                match typ:

                    # ── CRUD ──────────────────────────────────────────────

                    case 'create':
                        row = WhatsappBusiness(user_id=body['user_id'], data=body.get('data', {}))
                        db.add(row)
                        await db.commit()
                        await db.refresh(row)
                        return JSONResponse(content={"success": True, "data": {"id": str(row.id)}}, status_code=200)

                    case 'get':
                        result = await db.execute(
                            select(WhatsappBusiness).where(WhatsappBusiness.id == uuid.UUID(body['id']))
                        )
                        row = result.scalar_one_or_none()
                        if not row:
                            raise Exception("record not found")
                        return JSONResponse(content={"success": True, "data": {
                            "id": str(row.id), "user_id": row.user_id, "data": row.data
                        }}, status_code=200)

                    case 'list':
                        result = await db.execute(
                            select(WhatsappBusiness).where(WhatsappBusiness.user_id == body['user_id'])
                        )
                        rows = result.scalars().all()
                        return JSONResponse(content={"success": True, "data": [
                            {"id": str(r.id), "data": r.data} for r in rows
                        ]}, status_code=200)

                    case 'update':
                        # 1. save to DB
                        row = await _get_row_by_user(db, body['user_id'])
                        if not row:
                            row = WhatsappBusiness(user_id=body['user_id'], data=body.get('data', {}))
                            db.add(row)
                        else:
                            if 'data' in body:
                                row.data = body['data']
                        await db.commit()
                        await db.refresh(row)

                        # 2. push profile to Meta in real-time
                        warning = None
                        meta     = _meta_cfg(row)
                        token    = meta.get('access_token')
                        phone_id = meta.get('phone_number_id')
                        if token and phone_id:
                            profile = (body.get('data') or {})
                            try:
                                status_code, meta_resp = await _push_profile_to_meta(token, phone_id, profile)
                                if status_code != 200:
                                    warning = f"Saved to DB but Meta profile update failed: {meta_resp}"
                            except Exception as e:
                                warning = f"Saved to DB but Meta profile update failed: {e}"
                        else:
                            warning = "Saved to DB. Meta profile not pushed — credentials not configured."

                        resp = {"success": True, "data": {"id": str(row.id)}}
                        if warning:
                            resp["warning"] = warning
                        return JSONResponse(content=resp, status_code=200)

                    case 'delete':
                        result = await db.execute(
                            select(WhatsappBusiness).where(WhatsappBusiness.id == uuid.UUID(body['id']))
                        )
                        row = result.scalar_one_or_none()
                        if not row:
                            raise Exception("record not found")
                        await db.delete(row)
                        await db.commit()
                        return JSONResponse(content={"success": True, "data": {}}, status_code=200)

                    # ── Meta config save + webhook subscription ────────────

                    case 'meta_config_save':
                        row = await _get_row(db, body)
                        if not row:
                            raise Exception("record not found — create first")
                        d   = dict(row.data or {})
                        cfg = dict(d.get('config', {}))
                        if 'meta' in body:
                            meta_payload = body['meta']
                        else:
                            src = body.get('data', {})
                            meta_payload = {
                                'access_token':    src.get('meta_access_token', src.get('access_token', '')),
                                'waba_id':         src.get('waba_id', ''),
                                'phone_number_id': src.get('phone_number_id', ''),
                                'catalog_id':      src.get('catalog_id', ''),
                            }
                        cfg['meta'] = meta_payload
                        if body.get('product_dir_token'):
                            cfg['product_dir_token'] = body['product_dir_token']
                        if body.get('base_url'):
                            cfg['base_url'] = body['base_url']
                        d['config'] = cfg
                        row.data = d
                        await db.commit()

                        # subscribe webhook after saving
                        warning = None
                        token   = meta_payload.get('access_token')
                        waba_id = meta_payload.get('waba_id')
                        if token and waba_id:
                            try:
                                sc, sr = await _subscribe_webhook(token, waba_id, ['messages'])
                                if sc != 200:
                                    warning = f"Credentials saved but webhook subscription failed: {sr}"
                            except Exception as e:
                                warning = f"Credentials saved but webhook subscription failed: {e}"

                        resp = {"success": True, "data": {}}
                        if warning:
                            resp["warning"] = warning
                        return JSONResponse(content=resp, status_code=200)

                    # alias
                    case 'save_meta_config':
                        row = await _get_row(db, body)
                        if not row:
                            raise Exception("record not found — create first")
                        d   = dict(row.data or {})
                        cfg = dict(d.get('config', {}))
                        if 'meta' in body:
                            meta_payload = body['meta']
                        else:
                            src = body.get('data', {})
                            meta_payload = {
                                'access_token':    src.get('meta_access_token', src.get('access_token', '')),
                                'waba_id':         src.get('waba_id', ''),
                                'phone_number_id': src.get('phone_number_id', ''),
                                'catalog_id':      src.get('catalog_id', ''),
                            }
                        cfg['meta'] = meta_payload
                        d['config'] = cfg
                        row.data = d
                        await db.commit()

                        warning = None
                        token   = meta_payload.get('access_token')
                        waba_id = meta_payload.get('waba_id')
                        if token and waba_id:
                            try:
                                sc, sr = await _subscribe_webhook(token, waba_id, ['messages'])
                                if sc != 200:
                                    warning = f"Credentials saved but webhook subscription failed: {sr}"
                            except Exception as e:
                                warning = f"Credentials saved but webhook subscription failed: {e}"

                        resp = {"success": True, "data": {}}
                        if warning:
                            resp["warning"] = warning
                        return JSONResponse(content=resp, status_code=200)

                    # ── Get Meta config ────────────────────────────────────

                    case 'get_meta_config':
                        row = await _get_row(db, body)
                        if not row:
                            raise Exception("record not found")
                        meta  = _meta_cfg(row)
                        token = meta.get('access_token', '')
                        masked = token[-6:] if token else ''
                        return JSONResponse(content={"success": True, "data": {
                            "waba_id":             meta.get('waba_id', ''),
                            "phone_number_id":     meta.get('phone_number_id', ''),
                            "catalog_id":          meta.get('catalog_id', ''),
                            "access_token_masked": masked,
                            "access_token_set":    bool(token),
                        }}, status_code=200)

                    # ── Meta test ──────────────────────────────────────────

                    case 'meta_test':
                        row = await _get_row(db, body)
                        if not row:
                            raise Exception("record not found")
                        meta     = _meta_cfg(row)
                        token    = meta.get('access_token')
                        phone_id = meta.get('phone_number_id')
                        waba_id  = meta.get('waba_id')
                        if not token:
                            raise Exception("meta.access_token not configured")
                        results = {}
                        async with httpx.AsyncClient() as client:
                            r  = await client.get("https://graph.facebook.com/v18.0/me",
                                                  params={"access_token": token})
                            results['token'] = r.json()
                            if phone_id:
                                r2 = await client.get(f"https://graph.facebook.com/v18.0/{phone_id}",
                                                      params={"access_token": token})
                                results['phone_number'] = r2.json()
                            if waba_id:
                                r3 = await client.get(f"https://graph.facebook.com/v18.0/{waba_id}",
                                                      params={"access_token": token})
                                results['waba'] = r3.json()
                        return JSONResponse(content={"success": True, "data": results}, status_code=200)

                    # ── Meta sync profile ──────────────────────────────────

                    case 'meta_sync_profile':
                        row = await _get_row(db, body)
                        if not row:
                            raise Exception("record not found")
                        meta     = _meta_cfg(row)
                        token    = meta.get('access_token')
                        phone_id = meta.get('phone_number_id')
                        if not token or not phone_id:
                            raise Exception("meta.access_token and meta.phone_number_id are required")
                        d       = row.data or {}
                        profile = d.get('profile', d)
                        sc, meta_resp = await _push_profile_to_meta(token, phone_id, profile)
                        return JSONResponse(content={"success": True, "data": {"meta_response": meta_resp}},
                                            status_code=200)

                    # ── Catalog sync — per-product with health status ───────

                    case 'catalog_sync':
                        row = await _get_row(db, body)
                        if not row:
                            raise Exception("record not found")
                        meta       = _meta_cfg(row)
                        token      = meta.get('access_token')
                        catalog_id = meta.get('catalog_id')
                        if not token or not catalog_id:
                            raise Exception("meta.access_token and meta.catalog_id are required")

                        d        = row.data or {}
                        cfg      = d.get('config', {})
                        pd_token = body.get('access_token', cfg.get('product_dir_token', ''))
                        base_url = body.get('base_url', cfg.get('base_url', 'http://localhost:8000'))

                        pd_resp  = await _fetch_products(pd_token, base_url)
                        products = pd_resp.get('data', {}).get('hits', pd_resp.get('data', []))
                        if isinstance(products, dict):
                            products = products.get('hits', [])

                        success_count = 0
                        fail_count    = 0
                        failed_items  = []

                        async with httpx.AsyncClient() as client:
                            for p in products:
                                doc = p.get('document', p)
                                try:
                                    sc, resp_json = await _push_product_to_meta(client, token, catalog_id, doc)
                                    if sc in (200, 201):
                                        success_count += 1
                                    else:
                                        fail_count += 1
                                        if len(failed_items) < 20:
                                            failed_items.append({
                                                "id":     str(doc.get('id', '')),
                                                "name":   doc.get('name', ''),
                                                "reason": str(resp_json),
                                            })
                                except Exception as e:
                                    fail_count += 1
                                    if len(failed_items) < 20:
                                        failed_items.append({
                                            "id":     str(doc.get('id', '')),
                                            "name":   doc.get('name', ''),
                                            "reason": str(e),
                                        })

                        total        = len(products)
                        sync_health  = "good" if fail_count == 0 else ("partial" if success_count > 0 else "failed")
                        catalog_log  = {
                            "catalog_id":     catalog_id,
                            "last_sync":      datetime.now(timezone.utc).isoformat(),
                            "total_products": total,
                            "synced":         success_count,
                            "failed":         fail_count,
                            "failed_items":   failed_items,
                            "sync_health":    sync_health,
                        }
                        d = dict(row.data or {})
                        d['catalog'] = catalog_log
                        row.data = d
                        await db.commit()

                        return JSONResponse(content={"success": True, "data": catalog_log}, status_code=200)

                    case 'catalog_status':
                        row = await _get_row(db, body)
                        if not row:
                            raise Exception("record not found")
                        d = row.data or {}
                        return JSONResponse(content={"success": True,
                                                     "data": d.get('catalog', {
                                                         "last_sync": None, "total_products": 0,
                                                         "synced": 0, "failed": 0,
                                                         "sync_health": "unknown"
                                                     })}, status_code=200)

                    # ── Webhook event ──────────────────────────────────────

                    case 'webhook_event':
                        payload = await request.json()
                        row     = await _get_first_business_row(db)
                        if row:
                            await _log(db, 'event', '', '', 'webhook', payload, str(row.id))

                        for entry in payload.get('entry', []):
                            for change in entry.get('changes', []):
                                value    = change.get('value', {})

                                # delivery status updates
                                for status_obj in value.get('statuses', []):
                                    wa_id  = status_obj.get('id', '')
                                    status = status_obj.get('status', '')  # sent/delivered/read
                                    if wa_id and status:
                                        log_row = await db.execute(
                                            select(WhatsappLog).where(WhatsappLog.wa_msg_id == wa_id)
                                        )
                                        log_entry = log_row.scalar_one_or_none()
                                        if log_entry:
                                            log_entry.msg_status = status
                                            await db.flush()

                                # inbound messages
                                for msg in value.get('messages', []):
                                    from_number = msg.get('from', '')
                                    msg_id      = msg.get('id', '')
                                    msg_type    = msg.get('type', '')
                                    print(f"[webhook] from={from_number} type={msg_type} msg_id={msg_id}")
                                    if msg_type == 'text':
                                        msg_text = msg.get('text', {}).get('body', '')
                                    elif msg_type == 'interactive':
                                        inter    = msg.get('interactive', {})
                                        msg_text = (
                                            inter.get('list_reply', {}).get('title', '') or
                                            inter.get('button_reply', {}).get('title', '') or
                                            inter.get('list_reply', {}).get('id', '')
                                        )
                                        # prefix with 'product:' so handler knows it's a product selection
                                        if inter.get('list_reply'):
                                            msg_text = f"product:{inter.get('list_reply', {}).get('title', '')}"
                                    else:
                                        msg_text = msg_type
                                    print(f"[webhook] msg_text={msg_text} row={row}")
                                    if row and from_number and msg_text:
                                        await _handle_incoming_message(db, row, from_number, msg_text, msg_id)

                        await db.commit()
                        return PlainTextResponse("OK", status_code=200)

                    # ── Automation config + webhook subscription ────────────

                    case 'get_automation_config':
                        row = await _get_row(db, body)
                        if not row:
                            raise Exception("record not found")
                        return JSONResponse(content={"success": True,
                                                     "data": (row.data or {}).get('automation', {})},
                                            status_code=200)

                    case 'save_automation_config':
                        row = await _get_row(db, body)
                        if not row:
                            raise Exception("record not found")
                        d = dict(row.data or {})
                        d['automation'] = body.get('data', {})
                        row.data = d
                        await db.commit()

                        # subscribe/unsubscribe webhook based on auto_reply toggle
                        warning  = None
                        meta     = _meta_cfg(row)
                        token    = meta.get('access_token')
                        waba_id  = meta.get('waba_id')
                        auto_reply = body.get('data', {}).get('toggles', {}).get('auto_reply', True)
                        if token and waba_id:
                            try:
                                if auto_reply:
                                    sc, sr = await _subscribe_webhook(token, waba_id, ['messages'])
                                    if sc != 200:
                                        warning = f"Config saved but webhook subscription failed: {sr}"
                            except Exception as e:
                                warning = f"Config saved but webhook operation failed: {e}"

                        resp = {"success": True, "data": {}}
                        if warning:
                            resp["warning"] = warning
                        return JSONResponse(content=resp, status_code=200)

                    # ── Conversation list ───────────────────────────────────

                    case 'conversation_list':
                        result = await db.execute(
                            select(WhatsappLog)
                            .where(WhatsappLog.direction == 'in')
                            .order_by(WhatsappLog.created_at.desc())
                        )
                        logs = result.scalars().all()
                        contacts_map = {}
                        for l in logs:
                            phone = l.from_ or ''
                            if phone not in contacts_map:
                                contacts_map[phone] = {
                                    "phone":        phone,
                                    "name":         phone,
                                    "last_message": l.message,
                                    "last_seen":    l.created_at.isoformat() if l.created_at else None,
                                    "count":        0,
                                }
                            contacts_map[phone]['count'] += 1
                        contacts = list(contacts_map.values())
                        r_in  = await db.execute(select(WhatsappLog).where(WhatsappLog.direction == 'in'))
                        r_out = await db.execute(select(WhatsappLog).where(WhatsappLog.direction == 'out'))
                        return JSONResponse(content={"success": True, "data": {
                            "contacts": contacts,
                            "stats": {
                                "total_contacts": len(contacts),
                                "total_received": len(r_in.scalars().all()),
                                "total_sent":     len(r_out.scalars().all()),
                            },
                        }}, status_code=200)

                    # ── Conversation messages ───────────────────────────────

                    case 'conversation_messages':
                        phone = body.get('phone', '')
                        if not phone:
                            raise Exception("phone is required")
                        result = await db.execute(
                            select(WhatsappLog)
                            .where((WhatsappLog.from_ == phone) | (WhatsappLog.to_ == phone))
                            .order_by(WhatsappLog.created_at.asc())
                        )
                        logs = result.scalars().all()
                        return JSONResponse(content={"success": True, "data": [{
                            "id":         str(l.id),
                            "direction":  l.direction,
                            "from":       l.from_,
                            "to":         l.to_,
                            "message":    l.message,
                            "created_at": l.created_at.isoformat() if l.created_at else None,
                            "status":     l.msg_status or "sent",
                        } for l in logs]}, status_code=200)

                    # ── Get logs ────────────────────────────────────────────

                    case 'get_logs':
                        limit  = int(body.get('limit', 50))
                        result = await db.execute(
                            select(WhatsappLog).order_by(WhatsappLog.created_at.desc()).limit(limit)
                        )
                        logs = result.scalars().all()
                        return JSONResponse(content={"success": True, "data": [{
                            "id":         str(l.id),
                            "direction":  l.direction,
                            "from":       l.from_,
                            "to":         l.to_,
                            "message":    l.message,
                            "status":     l.msg_status or "sent",
                            "created_at": l.created_at.isoformat() if l.created_at else None,
                        } for l in logs]}, status_code=200)

                    # ── Send message ────────────────────────────────────────

                    case 'send_message':
                        row = await _get_row(db, body)
                        if not row:
                            raise Exception("record not found")
                        meta     = _meta_cfg(row)
                        token    = meta.get('access_token')
                        phone_id = meta.get('phone_number_id')
                        if not token or not phone_id:
                            raise Exception("meta credentials not configured")
                        to_num = body['to']
                        text   = body['message']
                        result = await _send_text(token, phone_id, to_num, text)
                        wa_id  = result.get('messages', [{}])[0].get('id') if isinstance(result, dict) else None
                        await _log(db, 'out', phone_id, to_num, text, result, str(row.id), wa_id)
                        await db.commit()
                        return JSONResponse(content={"success": True, "data": {}}, status_code=200)

                    case _:
                        raise Exception(f"unknown typ={typ}")

        except Exception as e:
            print(f"[701/i] ERROR: {e}")
            return _err("Err [i]", e.args)


    # ---- i_init ----
    async def i_init(request: Request):
        return


    # ---- get_schema_for_create ----
    async def get_schema_for_create(request: Request):
        return {
            'body': {
                "type": "object", "required": ["data"],
                "properties": {
                    "data": {
                        "type": "object", "required": ["config"],
                        "properties": {
                            "config": {
                                "type": "object", "required": ["database"],
                                "properties": {
                                    "database": {"type": "object", "required": ["url"],
                                                 "properties": {"url": {"type": "string"}}},
                                    "meta": {"type": "object", "properties": {
                                        "access_token":    {"type": "string"},
                                        "waba_id":         {"type": "string"},
                                        "phone_number_id": {"type": "string"},
                                        "catalog_id":      {"type": "string"},
                                    }}
                                }
                            }
                        }
                    }
                },
                "example": {"data": {"config": {
                    "database": {"url": "postgresql+asyncpg://user:pass@localhost:5432/db"},
                    "meta": {"access_token": "", "waba_id": "", "phone_number_id": "", "catalog_id": ""}
                }}}
            },
            'querystring': {}
        }


    # ---- get_schema_for_run ----
    async def get_schema_for_run(request: Request):
        typ = dict(request.query_params).get('typ')
        _any_id = {"id": {"type": "string"}, "user_id": {"type": "string"}}
        _schemas = {
            'create':               {"type": "object", "required": ["user_id"],
                                     "properties": {"user_id": {"type": "string"}, "data": {"type": "object"}}},
            'get':                  {"type": "object", "required": ["id"],
                                     "properties": {"id": {"type": "string"}}},
            'list':                 {"type": "object", "required": ["user_id"],
                                     "properties": {"user_id": {"type": "string"}}},
            'update':               {"type": "object", "required": ["user_id"],
                                     "properties": {"user_id": {"type": "string"}, "data": {"type": "object"}}},
            'delete':               {"type": "object", "required": ["id"],
                                     "properties": {"id": {"type": "string"}}},
            'meta_config_save':     {"type": "object", "properties": {**_any_id, "meta": {"type": "object"}, "data": {"type": "object"}}},
            'save_meta_config':     {"type": "object", "properties": {**_any_id, "meta": {"type": "object"}, "data": {"type": "object"}}},
            'get_meta_config':      {"type": "object", "properties": _any_id},
            'meta_test':            {"type": "object", "properties": _any_id},
            'meta_sync_profile':    {"type": "object", "properties": _any_id},
            'catalog_sync':         {"type": "object", "properties": {**_any_id,
                                     "access_token": {"type": "string"}, "base_url": {"type": "string"}}},
            'catalog_status':       {"type": "object", "properties": _any_id},
            'webhook_event':        {"type": "object", "properties": {}},
            'get_automation_config':{"type": "object", "properties": _any_id},
            'save_automation_config':{"type": "object", "required": ["data"],
                                      "properties": {**_any_id, "data": {"type": "object"}}},
            'conversation_list':    {"type": "object", "properties": _any_id},
            'conversation_messages':{"type": "object", "required": ["phone"],
                                     "properties": {**_any_id, "phone": {"type": "string"}}},
            'get_logs':             {"type": "object", "properties": {"limit": {"type": "integer"}}},
            'send_message':         {"type": "object", "required": ["to", "message"],
                                     "properties": {**_any_id, "to": {"type": "string"},
                                                    "message": {"type": "string"}}},
        }
        if typ not in _schemas:
            raise Exception(f"no body schema found for [typ={typ}]")
        return {
            "body": _schemas[typ],
            "querystring": {
                "type": "object", "required": ["typ"],
                "properties": {"typ": {"type": "string", "enum": list(_schemas.keys())}}
            }
        }


    # ---- get_doc_for_run ----
    async def get_doc_for_run(request: Request):
        _var = {"ep_name": f"client/api/i/{_p['data']['instance'].project.name}/{_p['data']['instance'].utility.name}"}
        _typs = list((await get_schema_for_run(fake_req_obj(
            method="POST", url="", headers={}, query_params={"typ": "create"},
            path_params={}, json_data={}, state={}
        )))['querystring']['properties']['typ']['enum'])

        paths, schemas = {}, {}
        for typ in _typs:
            paths[f"/{_var['ep_name']}?typ={typ}"] = {
                "post": {"summary": typ,
                         "requestBody": {"required": True, "content": {"application/json": {"schema": {"$ref": f"#/components/schemas/{typ}"}}}},
                         "responses": {"200": {"description": "Successful Response"}}}
            }
            schemas[typ] = (await get_schema_for_run(fake_req_obj(
                method="POST", url="", headers={}, query_params={"typ": typ},
                path_params={}, json_data={}, state={}
            )))['body']

        return {
            "openapi": "3.0.3",
            "info": {"title": "[WhatsApp Commerce] api-docs",
                     "description": f"Project={_p['data']['instance'].project.name}, Instance={_p['data']['instance'].name}, Utility-id={_p['data']['instance'].utility.id}",
                     "version": "1.0.0"},
            "paths": paths,
            "components": {"schemas": schemas}
        }


    return i, get_schema_for_create, get_schema_for_run, i_init, get_doc_for_run
