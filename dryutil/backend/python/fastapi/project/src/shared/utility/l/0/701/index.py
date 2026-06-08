from typing import Any
from fastapi import Request
from fastapi.responses import JSONResponse, PlainTextResponse
from sqlalchemy import Column, String, Text, DateTime, Integer, func as sa_func
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.orm import declarative_base
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.future import select
import uuid
import httpx
from datetime import datetime, timezone, timedelta
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

# ── New CRM tables ─────────────────────────────────────────────────────────

class WhatsappMessage(Base):
    """Structured message store (req 1)"""
    __tablename__ = "whatsapp_messages"
    id            = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    wa_id         = Column(String(100), nullable=True, index=True)   # Meta message id
    phone_number  = Column(String(50), nullable=False, index=True)
    customer_name = Column(String(200), nullable=True)
    message_text  = Column(Text, nullable=True)
    message_type  = Column(String(30), nullable=True, default="text")  # text/interactive/image/…
    direction     = Column(String(10), nullable=False)                 # incoming/outgoing
    timestamp     = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), index=True)

class WhatsappCustomer(Base):
    """Auto-managed customer CRM (req 2)"""
    __tablename__ = "whatsapp_customers"
    id             = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    wa_id          = Column(String(100), nullable=True)
    phone_number   = Column(String(50), nullable=False, unique=True, index=True)
    name           = Column(String(200), nullable=True)
    first_seen     = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    last_seen      = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    total_messages = Column(Integer, default=0)

class WhatsappProductRequest(Base):
    """Tracks every product match served via webhook — powers product_analytics"""
    __tablename__ = "whatsapp_product_requests"
    id           = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    business_id  = Column(String(155), nullable=True, index=True)
    phone_number = Column(String(50), nullable=True, index=True)
    product_name = Column(String(300), nullable=False, index=True)
    query_text   = Column(Text, nullable=True)   # original message that triggered this
    created_at   = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), index=True)

# ── SaaS Catalog Sync tables ───────────────────────────────────────────────

class CatalogSyncHistory(Base):
    """One row per full sync run per seller."""
    __tablename__ = "catalog_sync_history"
    id            = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    business_id   = Column(String(155), nullable=False, index=True)  # whatsapp_business.id
    catalog_id    = Column(String(155), nullable=True)
    status        = Column(String(20), nullable=False, default="running")  # running/completed/failed
    total         = Column(Integer, default=0)
    synced        = Column(Integer, default=0)
    failed        = Column(Integer, default=0)
    started_at    = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    completed_at  = Column(DateTime(timezone=True), nullable=True)
    trigger       = Column(String(30), nullable=True, default="manual")  # manual/scheduled/api

class CatalogSyncLog(Base):
    """One row per product per sync run."""
    __tablename__ = "catalog_sync_log"
    id            = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    sync_id       = Column(UUID(as_uuid=True), nullable=False, index=True)  # → CatalogSyncHistory.id
    business_id   = Column(String(155), nullable=False, index=True)
    product_id    = Column(String(300), nullable=True)
    product_name  = Column(String(300), nullable=True)
    status        = Column(String(20), nullable=False)   # synced/failed/skipped
    error         = Column(Text, nullable=True)
    meta_response = Column(JSONB, nullable=True)
    created_at    = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))


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

    # ── SaaS Catalog helpers ────────────────────────────────────────────────

    def _normalize_product(doc: dict) -> dict:
        """Convert product_dir format → Meta Catalog API payload."""
        price_raw = doc.get('variant_mrp') or doc.get('variant_prices') or doc.get('price', 0)
        if isinstance(price_raw, list):
            price_raw = price_raw[0] if price_raw else 0
        try:
            price_cents = int(float(str(price_raw).replace(',', '').strip()) * 100)
        except Exception:
            price_cents = 0
        retailer_id = str(doc.get('id', doc.get('slug', '')))
        name        = str(doc.get('title', doc.get('name', '')))[:100]
        description = str(doc.get('description', name))[:200] or name
        # extract image from metadata.color[].image[] — join split URL parts
        image_url = ''
        metadata = doc.get('metadata', {})
        colors = metadata.get('color', metadata.get('colors', []))
        for color in colors:
            images = color.get('image', [])
            # collect all url parts and try to build a complete URL
            parts = [img.get('url', '') if isinstance(img, dict) else str(img) for img in images]
            # find a part that starts with https:// and a part ending with image extension
            base = next((p for p in parts if p.startswith('https://')), '')
            ext  = next((p for p in parts if any(p.endswith(e) for e in ['.jpg', '.jpeg', '.png', '.webp'])), '')
            if base and ext:
                image_url = base + '/' + ext.lstrip('/')
                break
            elif base and '.' in base.split('/')[-1]:
                image_url = base
                break
        # fallback to direct fields
        if not image_url:
            for img_key in ['image_url', 'image', 'thumbnail', 'photo']:
                val = doc.get(img_key, '')
                if isinstance(val, list) and val:
                    val = val[0]
                if isinstance(val, str) and val.startswith('https://'):
                    image_url = val
                    break
        # last fallback
        if not image_url:
            image_url = 'https://placehold.co/400x400/png'
        product_url = doc.get('url', metadata.get('url', 'https://onamoda.in/'))
        return {
            "retailer_id":  retailer_id,
            "name":         name,
            "description":  description,
            "price":        price_cents,
            "currency":     doc.get('currency', metadata.get('variant', [{}])[0].get('currency', 'INR') if metadata.get('variant') else 'INR'),
            "availability": "in stock",
            "url":          product_url,
            "image_url":    image_url,
        }

    async def _meta_push_one(client: httpx.AsyncClient, token: str, catalog_id: str, payload: dict):
        """POST one product to Meta Catalog. Returns (status_code, json)."""
        r = await client.post(
            f"https://graph.facebook.com/v19.0/{catalog_id}/products",
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
            json=payload,
            timeout=15,
        )
        return r.status_code, r.json()

    async def _meta_delete_one(client: httpx.AsyncClient, token: str, catalog_id: str, retailer_id: str):
        """DELETE one product from Meta Catalog by retailer_id."""
        r = await client.delete(
            f"https://graph.facebook.com/v19.0/{catalog_id}/products",
            headers={"Authorization": f"Bearer {token}"},
            params={"retailer_id": retailer_id},
            timeout=15,
        )
        return r.status_code, r.json()

    async def _meta_discover_assets(token: str) -> dict:
        """
        Auto-discover WABA, phone numbers and catalogs from a token.
        Used by meta_connect — lays the groundwork for Embedded Signup.
        Returns dict with waba_list, phone_list, catalog_list.
        """
        result = {"waba_list": [], "phone_list": [], "catalog_list": [], "token_info": {}}
        async with httpx.AsyncClient() as client:
            # token identity
            r = await client.get("https://graph.facebook.com/v19.0/me",
                                  params={"access_token": token})
            result["token_info"] = r.json()
            user_id = result["token_info"].get("id", "")

            # WABAs the token has access to
            r2 = await client.get(
                f"https://graph.facebook.com/v19.0/{user_id}/businesses",
                params={"access_token": token, "fields": "id,name,whatsapp_business_accounts"}
            )
            businesses = r2.json().get("data", [])
            for biz in businesses:
                for waba in biz.get("whatsapp_business_accounts", {}).get("data", []):
                    waba_id = waba.get("id")
                    result["waba_list"].append({"id": waba_id, "name": waba.get("name", "")})

                    # phone numbers under this WABA
                    r3 = await client.get(
                        f"https://graph.facebook.com/v19.0/{waba_id}/phone_numbers",
                        params={"access_token": token, "fields": "id,display_phone_number,verified_name"}
                    )
                    for ph in r3.json().get("data", []):
                        result["phone_list"].append({
                            "id":           ph.get("id"),
                            "phone":        ph.get("display_phone_number"),
                            "display_name": ph.get("verified_name"),
                            "waba_id":      waba_id,
                        })

                # catalogs under this business
                r4 = await client.get(
                    f"https://graph.facebook.com/v19.0/{biz.get('id')}/owned_product_catalogs",
                    params={"access_token": token, "fields": "id,name"}
                )
                for cat in r4.json().get("data", []):
                    result["catalog_list"].append({"id": cat.get("id"), "name": cat.get("name", "")})

        return result

    # ── Product dir helper ──────────────────────────────────────────────────

    async def _fetch_products(token: str, base_url: str, q: str = "*", per_page: int = 250):
        print(f"[fetch_products] calling base_url={base_url}")
        async with httpx.AsyncClient() as client:
            # try public route first (no JWT needed)
            resp = await client.post(
                f"{base_url}/client-public/api/i/ona/product_dir?typ=get_product_list",
                json={"q": q, "per_page": per_page, "page": 1},
                timeout=30
            )
            if resp.status_code == 404:
                # fallback to authenticated route
                resp = await client.post(
                    f"{base_url}/client/api/i/ona/product_dir?typ=get_product_list",
                    headers={"Authorization": f"Bearer {token}"},
                    json={"q": q, "per_page": per_page, "page": 1},
                    timeout=30
                )
        print(f"[fetch_products] status={resp.status_code} body={resp.text[:300]}")
        return resp.json()

    # ── Log helper ──────────────────────────────────────────────────────────

    async def _upsert_customer(db, phone: str, name: str = None, wa_id: str = None):
        """Auto create/update customer record."""
        result = await db.execute(
            select(WhatsappCustomer).where(WhatsappCustomer.phone_number == phone)
        )
        customer = result.scalar_one_or_none()
        now = datetime.now(timezone.utc)
        if customer:
            customer.last_seen = now
            customer.total_messages = (customer.total_messages or 0) + 1
            if name and not customer.name:
                customer.name = name
            if wa_id and not customer.wa_id:
                customer.wa_id = wa_id
        else:
            customer = WhatsappCustomer(
                phone_number=phone,
                name=name or phone,
                wa_id=wa_id,
                first_seen=now,
                last_seen=now,
                total_messages=1,
            )
            db.add(customer)
        await db.flush()
        return customer

    async def _log(db, direction: str, from_: str, to_: str, message: str, payload: dict,
                   business_id: str = None, wa_msg_id: str = None,
                   message_type: str = "text", customer_name: str = None):
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
        # write to whatsapp_messages
        phone = from_ if direction in ('in', 'incoming') else to_
        msg_direction = "incoming" if direction == 'in' else "outgoing"
        wa_msg = WhatsappMessage(
            wa_id=wa_msg_id,
            phone_number=phone,
            customer_name=customer_name,
            message_text=message,
            message_type=message_type,
            direction=msg_direction,
        )
        db.add(wa_msg)
        # upsert customer only for real messages (not event logs)
        if direction in ('in', 'out') and phone:
            await _upsert_customer(db, phone, customer_name, wa_msg_id)
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

        async def _track_product_requests(products_shown: list, query: str):
            """Log each product served to WhatsappProductRequest for analytics."""
            for p in products_shown:
                doc  = p.get('document', p)
                name = doc.get('name', doc.get('title', ''))
                if name:
                    db.add(WhatsappProductRequest(
                        business_id=str(row.id),
                        phone_number=from_number,
                        product_name=str(name)[:300],
                        query_text=query,
                    ))
            await db.flush()

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
                    await _track_product_requests(products_to_show[:10], product_name)
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
                await _track_product_requests(products[:10], text)
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
                        await _track_product_requests(products[:5], msg_text.strip())
                        await _log(db, 'out', phone_id, from_number, reply, r, str(row.id))
                    else:
                        r = await _send_product_list(token, phone_id, from_number, products,
                                                     f"Results for '{msg_text.strip()}'")
                        await _track_product_requests(products[:10], msg_text.strip())
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
                        row = await _get_row(db, body)
                        if not row:
                            row = WhatsappBusiness(user_id=body.get('user_id', str(uuid.uuid4())), data=body.get('data', {}))
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

                    # ── meta_oauth_start — generate Meta OAuth URL ──────────────────

                    case 'meta_oauth_start':
                        row = await _get_row(db, body)
                        if not row:
                            raise Exception("record not found")
                        d   = row.data or {}
                        cfg = d.get('config', {})
                        app_id       = cfg.get('meta_app_id', '')
                        redirect_uri = body.get('redirect_uri', cfg.get('oauth_redirect_uri', ''))
                        state        = body.get('state', str(row.id))  # use business id as state
                        if not app_id:
                            raise Exception("meta_app_id not configured in instance config")
                        if not redirect_uri:
                            raise Exception("redirect_uri required")
                        scope = 'whatsapp_business_management,whatsapp_business_messaging,business_management,catalog_management'
                        auth_url = (
                            f"https://www.facebook.com/v19.0/dialog/oauth"
                            f"?client_id={app_id}"
                            f"&redirect_uri={redirect_uri}"
                            f"&scope={scope}"
                            f"&state={state}"
                            f"&response_type=code"
                        )
                        return JSONResponse(content={"success": True, "data": {
                            "auth_url": auth_url,
                            "state":    state,
                        }}, status_code=200)

                    # ── meta_oauth_callback — exchange code, discover assets, save ──

                    case 'meta_oauth_callback':
                        row = await _get_row(db, body)
                        if not row:
                            raise Exception("record not found")
                        d   = dict(row.data or {})
                        cfg = dict(d.get('config', {}))
                        app_id       = cfg.get('meta_app_id', '')
                        app_secret   = cfg.get('meta_app_secret', '')
                        redirect_uri = body.get('redirect_uri', cfg.get('oauth_redirect_uri', ''))
                        code         = body.get('code', '')
                        if not code:
                            raise Exception("authorization code is required")
                        if not app_id or not app_secret:
                            raise Exception("meta_app_id and meta_app_secret not configured")

                        # 1. exchange code for short-lived token
                        async with httpx.AsyncClient() as client:
                            r = await client.get(
                                "https://graph.facebook.com/v19.0/oauth/access_token",
                                params={
                                    "client_id":     app_id,
                                    "client_secret": app_secret,
                                    "redirect_uri":  redirect_uri,
                                    "code":          code,
                                },
                                timeout=15
                            )
                        token_data = r.json()
                        if "error" in token_data:
                            raise Exception(f"Token exchange failed: {token_data['error'].get('message')}")
                        short_token = token_data.get("access_token", "")

                        # 2. exchange for long-lived token
                        async with httpx.AsyncClient() as client:
                            r2 = await client.get(
                                "https://graph.facebook.com/v19.0/oauth/access_token",
                                params={
                                    "grant_type":    "fb_exchange_token",
                                    "client_id":     app_id,
                                    "client_secret": app_secret,
                                    "fb_exchange_token": short_token,
                                },
                                timeout=15
                            )
                        ll_data = r2.json()
                        token = ll_data.get("access_token", short_token)

                        # 3. auto-discover all assets
                        assets = await _meta_discover_assets(token)

                        # 4. auto-select if only one of each
                        auto_waba    = assets["waba_list"][0]["id"]    if len(assets["waba_list"]) == 1    else None
                        auto_phone   = assets["phone_list"][0]["id"]   if len(assets["phone_list"]) == 1   else None
                        auto_catalog = assets["catalog_list"][0]["id"] if len(assets["catalog_list"]) == 1 else None

                        # 5. save config
                        existing_meta = cfg.get('meta', {})
                        cfg['meta'] = {
                            **existing_meta,
                            'access_token':    token,
                            'waba_id':         auto_waba    or existing_meta.get('waba_id', ''),
                            'phone_number_id': auto_phone   or existing_meta.get('phone_number_id', ''),
                            'catalog_id':      auto_catalog or existing_meta.get('catalog_id', ''),
                        }
                        cfg['oauth_connected_at'] = datetime.now(timezone.utc).isoformat()
                        d['config'] = cfg
                        row.data = d
                        await db.commit()

                        return JSONResponse(content={"success": True, "data": {
                            "connected":     True,
                            "discovered":    assets,
                            "auto_selected": {
                                "waba_id":         auto_waba,
                                "phone_number_id": auto_phone,
                                "catalog_id":      auto_catalog,
                            },
                            "needs_selection": (
                                len(assets["waba_list"]) > 1 or
                                len(assets["catalog_list"]) > 1
                            ),
                            "note": "If multiple assets found, call save_meta_config to set specific IDs."
                        }}, status_code=200)

                    # ── meta_connection_status — seller connection info ─────────────

                    case 'meta_connection_status':
                        row = await _get_row(db, body)
                        if not row:
                            raise Exception("record not found")
                        d    = row.data or {}
                        cfg  = d.get('config', {})
                        meta = cfg.get('meta', {})
                        token      = meta.get('access_token', '')
                        catalog_id = meta.get('catalog_id', '')
                        phone_id   = meta.get('phone_number_id', '')
                        connected  = bool(token)
                        business_name = catalog_name = phone_number = ''
                        if connected:
                            try:
                                async with httpx.AsyncClient() as client:
                                    if catalog_id:
                                        rc = await client.get(
                                            f"https://graph.facebook.com/v19.0/{catalog_id}",
                                            params={"access_token": token, "fields": "id,name"}
                                        )
                                        catalog_name = rc.json().get('name', '')
                                    if phone_id:
                                        rp = await client.get(
                                            f"https://graph.facebook.com/v19.0/{phone_id}",
                                            params={"access_token": token, "fields": "display_phone_number,verified_name"}
                                        )
                                        pd = rp.json()
                                        phone_number  = pd.get('display_phone_number', '')
                                        business_name = pd.get('verified_name', d.get('profile', {}).get('title', ''))
                            except Exception:
                                pass
                        return JSONResponse(content={"success": True, "data": {
                            "connected":     connected,
                            "business_name": business_name or d.get('profile', {}).get('title', ''),
                            "catalog_name":  catalog_name,
                            "catalog_id":    catalog_id,
                            "phone_number":  phone_number,
                            "waba_id":       meta.get('waba_id', ''),
                            "connected_at":  cfg.get('oauth_connected_at'),
                            "access_token_set": connected,
                        }}, status_code=200)

                    case 'meta_connect':
                        # Accepts just an access_token, discovers all WABA/phone/catalog
                        # assets automatically. Saves discovered config to the record.
                        # This is the foundation for Meta Embedded Signup flow.
                        row = await _get_row(db, body)
                        if not row:
                            raise Exception("record not found — create first")
                        token = body.get('access_token') or body.get('meta', {}).get('access_token')
                        if not token:
                            raise Exception("access_token is required")
                        try:
                            assets = await _meta_discover_assets(token)
                        except Exception as e:
                            return JSONResponse(content={"success": False,
                                "message": f"Meta asset discovery failed: {e}",
                                "data": {}}, status_code=200)
                        # auto-select first available assets if only one each
                        auto_waba    = assets["waba_list"][0]["id"]    if len(assets["waba_list"]) == 1    else None
                        auto_phone   = assets["phone_list"][0]["id"]   if len(assets["phone_list"]) == 1   else None
                        auto_catalog = assets["catalog_list"][0]["id"] if len(assets["catalog_list"]) == 1 else None
                        # persist token + any auto-selected IDs
                        d   = dict(row.data or {})
                        cfg = dict(d.get('config', {}))
                        existing_meta = cfg.get('meta', {})
                        cfg['meta'] = {
                            **existing_meta,
                            'access_token':    token,
                            'waba_id':         auto_waba    or existing_meta.get('waba_id', ''),
                            'phone_number_id': auto_phone   or existing_meta.get('phone_number_id', ''),
                            'catalog_id':      auto_catalog or existing_meta.get('catalog_id', ''),
                        }
                        d['config'] = cfg
                        row.data = d
                        await db.commit()
                        return JSONResponse(content={"success": True, "data": {
                            "discovered":   assets,
                            "auto_selected": {
                                "waba_id":         auto_waba,
                                "phone_number_id": auto_phone,
                                "catalog_id":      auto_catalog,
                            },
                            "note": "If multiple assets found, call save_meta_config to set specific IDs."
                        }}, status_code=200)

                    # ── catalog_validate — validate credentials + catalog access ───────

                    case 'catalog_validate':
                        row = await _get_row(db, body)
                        if not row:
                            raise Exception("record not found")
                        meta       = _meta_cfg(row)
                        token      = meta.get('access_token')
                        catalog_id = meta.get('catalog_id')
                        waba_id    = meta.get('waba_id')
                        token_ok = catalog_ok = waba_ok = False
                        async with httpx.AsyncClient() as client:
                            r = await client.get("https://graph.facebook.com/v19.0/me",
                                                  params={"access_token": token})
                            token_ok = "id" in r.json()
                            if catalog_id:
                                r2 = await client.get(
                                    f"https://graph.facebook.com/v19.0/{catalog_id}",
                                    params={"access_token": token, "fields": "id,name,product_count"}
                                )
                                catalog_ok = "id" in r2.json()
                            if waba_id:
                                r3 = await client.get(
                                    f"https://graph.facebook.com/v19.0/{waba_id}",
                                    params={"access_token": token, "fields": "id,name"}
                                )
                                waba_ok = "id" in r3.json()
                        all_ok = token_ok and catalog_ok and waba_ok
                        return JSONResponse(content={"success": True, "data": {
                            "meta_connected":     token_ok,
                            "catalog_accessible": catalog_ok,
                            "waba_connected":     waba_ok,
                            "permissions_valid":  all_ok,
                            "valid":              all_ok,
                        }}, status_code=200)

                    # ── catalog_details — catalog info + product count from Meta ───────

                    case 'catalog_details':
                        row = await _get_row(db, body)
                        if not row:
                            raise Exception("record not found")
                        meta       = _meta_cfg(row)
                        token      = meta.get('access_token')
                        catalog_id = meta.get('catalog_id')
                        phone_number_id = meta.get('phone_number_id', '')
                        if not token or not catalog_id:
                            raise Exception("access_token and catalog_id required")
                        async with httpx.AsyncClient() as client:
                            r = await client.get(
                                f"https://graph.facebook.com/v19.0/{catalog_id}",
                                params={"access_token": token,
                                        "fields": "id,name,product_count,vertical,da_display_settings"}
                            )
                            # fetch phone number display
                            phone_display = ''
                            if phone_number_id:
                                rp = await client.get(
                                    f"https://graph.facebook.com/v19.0/{phone_number_id}",
                                    params={"access_token": token, "fields": "display_phone_number"}
                                )
                                phone_display = rp.json().get('display_phone_number', '')
                        d = r.json()
                        if "error" in d:
                            raise Exception(d["error"].get("message", "Meta API error"))
                        # get last sync info
                        run = (await db.execute(
                            select(CatalogSyncHistory)
                            .where(CatalogSyncHistory.business_id == str(row.id))
                            .order_by(CatalogSyncHistory.started_at.desc())
                            .limit(1)
                        )).scalar_one_or_none()
                        return JSONResponse(content={"success": True, "data": {
                            "meta_connected":        bool(token),
                            "business_name":         (row.data or {}).get('profile', {}).get('title', ''),
                            "catalog_name":          d.get('name', ''),
                            "catalog_id":            d.get('id', catalog_id),
                            "phone_number":          phone_display,
                            "total_catalog_products": d.get('product_count', 0),
                            "last_updated":          run.completed_at.isoformat() if run and run.completed_at else None,
                        }}, status_code=200)

                    # ── catalog_sync_full — full sync with proper history tracking ─────

                    case 'catalog_sync_full':
                        row = await _get_row(db, body)
                        if not row:
                            raise Exception("record not found")
                        meta       = _meta_cfg(row)
                        token      = meta.get('access_token')
                        catalog_id = meta.get('catalog_id')
                        if not token or not catalog_id:
                            raise Exception("access_token and catalog_id are required")

                        d               = row.data or {}
                        cfg             = d.get('config', {})
                        pd_token        = cfg.get('product_dir_token', '')
                        base_url        = cfg.get('base_url', 'https://fastapi.dryutil.1mn.io')
                        trigger         = body.get('trigger', 'manual')
                        business_id_str = str(row.id)

                        # fetch products first — before opening sync session
                        try:
                            pd_resp  = await _fetch_products(pd_token, base_url, per_page=250)
                            products = pd_resp.get('data', {}).get('products',
                                       pd_resp.get('data', {}).get('hits', []))
                            if isinstance(products, dict):
                                products = products.get('hits', products.get('products', []))
                        except Exception as e:
                            raise Exception(f"Failed to fetch products: {e}")

                        success_count, fail_count = 0, 0
                        sync_id    = uuid.uuid4()
                        started_at = datetime.now(timezone.utc)
                        completed_at = started_at
                        final_status = "failed"

                        async with AsyncSession(_engine) as sync_db:
                            sync_run = CatalogSyncHistory(
                                id=sync_id,
                                business_id=business_id_str,
                                catalog_id=catalog_id,
                                status="running",
                                trigger=trigger,
                                total=len(products),
                            )
                            sync_db.add(sync_run)
                            await sync_db.flush()

                            async with httpx.AsyncClient() as client:
                                for p in products:
                                    doc     = p.get('document', p)
                                    payload = _normalize_product(doc)
                                    pid     = payload["retailer_id"]
                                    pname   = payload["name"]
                                    try:
                                        sc, resp_json = await _meta_push_one(client, token, catalog_id, payload)
                                        ok = sc in (200, 201)
                                        sync_db.add(CatalogSyncLog(
                                            sync_id=sync_id,
                                            business_id=business_id_str,
                                            product_id=pid,
                                            product_name=pname,
                                            status="synced" if ok else "failed",
                                            error=None if ok else str(resp_json),
                                            meta_response=resp_json,
                                        ))
                                        if ok:
                                            success_count += 1
                                        else:
                                            fail_count += 1
                                    except Exception as e:
                                        fail_count += 1
                                        sync_db.add(CatalogSyncLog(
                                            sync_id=sync_id,
                                            business_id=business_id_str,
                                            product_id=pid,
                                            product_name=pname,
                                            status="failed",
                                            error=str(e),
                                        ))

                            completed_at = datetime.now(timezone.utc)
                            final_status = "completed" if fail_count == 0 else ("partial" if success_count > 0 else "failed")
                            sync_run.synced       = success_count
                            sync_run.failed       = fail_count
                            sync_run.status       = final_status
                            sync_run.completed_at = completed_at
                            await sync_db.commit()

                        # update legacy catalog key using main db session
                        from sqlalchemy import text as sa_text
                        catalog_json = f'{{"last_sync": "{completed_at.isoformat()}", "total_products": {len(products)}, "synced": {success_count}, "failed": {fail_count}, "sync_health": "{final_status}"}}'
                        await db.execute(sa_text(
                            f"UPDATE whatsapp_business SET data = jsonb_set(data, '{{catalog}}', '{catalog_json}'::jsonb) WHERE id = '{business_id_str}'::uuid"
                        ))
                        await db.commit()

                        return JSONResponse(content={"success": True, "data": {
                            "sync_id":      str(sync_id),
                            "status":       final_status,
                            "total":        len(products),
                            "synced":       success_count,
                            "failed":       fail_count,
                            "started_at":   started_at.isoformat(),
                            "completed_at": completed_at.isoformat(),
                        }}, status_code=200)

                    # ── catalog_sync_product — sync a single product by id ────────────

                    case 'catalog_sync_product':
                        row = await _get_row(db, body)
                        if not row:
                            raise Exception("record not found")
                        meta       = _meta_cfg(row)
                        token      = meta.get('access_token')
                        catalog_id = meta.get('catalog_id')
                        if not token or not catalog_id:
                            raise Exception("access_token and catalog_id are required")
                        product = body.get('product')
                        if not product:
                            raise Exception("product object is required")
                        payload = _normalize_product(product)
                        async with httpx.AsyncClient() as client:
                            sc, resp_json = await _meta_push_one(client, token, catalog_id, payload)
                        ok = sc in (200, 201)
                        return JSONResponse(content={"success": ok, "data": {
                            "retailer_id":   payload["retailer_id"],
                            "meta_response": resp_json,
                            "status":        "synced" if ok else "failed",
                        }}, status_code=200)

                    # ── catalog_delete_product — remove product from Meta catalog ──────

                    case 'catalog_delete_product':
                        row = await _get_row(db, body)
                        if not row:
                            raise Exception("record not found")
                        meta       = _meta_cfg(row)
                        token      = meta.get('access_token')
                        catalog_id = meta.get('catalog_id')
                        if not token or not catalog_id:
                            raise Exception("access_token and catalog_id are required")
                        retailer_id = body.get('retailer_id') or body.get('product_id')
                        if not retailer_id:
                            raise Exception("retailer_id is required")
                        async with httpx.AsyncClient() as client:
                            sc, resp_json = await _meta_delete_one(client, token, catalog_id, retailer_id)
                        return JSONResponse(content={"success": sc in (200, 201, 204), "data": {
                            "retailer_id":   retailer_id,
                            "meta_response": resp_json,
                        }}, status_code=200)

                    # ── catalog_sync_history — list sync runs for a seller ────────────

                    case 'catalog_sync_history':
                        row = await _get_row(db, body)
                        if not row:
                            raise Exception("record not found")
                        limit  = int(body.get('limit', 20))
                        offset = int(body.get('offset', 0))
                        runs = (await db.execute(
                            select(CatalogSyncHistory)
                            .where(CatalogSyncHistory.business_id == str(row.id))
                            .order_by(CatalogSyncHistory.started_at.desc())
                            .limit(limit).offset(offset)
                        )).scalars().all()
                        return JSONResponse(content={"success": True, "data": {
                            "history": [{
                                "id":             str(r.id),
                                "date":           r.started_at.isoformat() if r.started_at else None,
                                "catalog_id":     r.catalog_id,
                                "status":         r.status,
                                "total_products": r.total,
                                "synced":         r.synced,
                                "failed":         r.failed,
                                "trigger":        r.trigger,
                                "started_at":     r.started_at.isoformat() if r.started_at else None,
                                "completed_at":   r.completed_at.isoformat() if r.completed_at else None,
                            } for r in runs],
                        }}, status_code=200)

                    # ── catalog_sync_status — latest sync run status ──────────────────

                    case 'catalog_sync_status':
                        row = await _get_row(db, body)
                        if not row:
                            raise Exception("record not found")
                        run = (await db.execute(
                            select(CatalogSyncHistory)
                            .where(CatalogSyncHistory.business_id == str(row.id))
                            .order_by(CatalogSyncHistory.started_at.desc())
                            .limit(1)
                        )).scalar_one_or_none()
                        if not run:
                            return JSONResponse(content={"success": True, "data": {
                                "last_sync_status": "never_synced",
                                "last_sync":        None,
                                "sync_health":      "unknown",
                                "status":           "never_synced",
                            }}, status_code=200)
                        health = "good" if run.failed == 0 else ("partial" if run.synced > 0 else "failed")
                        return JSONResponse(content={"success": True, "data": {
                            "sync_id":          str(run.id),
                            "last_sync_status": run.status,
                            "status":           run.status,
                            "last_sync":        run.completed_at.isoformat() if run.completed_at else None,
                            "sync_health":      health,
                            "total":            run.total,
                            "synced":           run.synced,
                            "failed":           run.failed,
                            "started_at":       run.started_at.isoformat() if run.started_at else None,
                        }}, status_code=200)

                    # ── catalog_sync_errors — failed products for a sync run ──────────

                    case 'catalog_sync_errors':
                        sync_id = body.get('sync_id')
                        row     = await _get_row(db, body)
                        if not row:
                            raise Exception("record not found")
                        if not sync_id:
                            run = (await db.execute(
                                select(CatalogSyncHistory)
                                .where(CatalogSyncHistory.business_id == str(row.id))
                                .order_by(CatalogSyncHistory.started_at.desc())
                                .limit(1)
                            )).scalar_one_or_none()
                            if not run:
                                return JSONResponse(content={"success": True,
                                    "data": {"errors": [], "total_errors": 0}}, status_code=200)
                            sync_id = str(run.id)
                        logs = (await db.execute(
                            select(CatalogSyncLog)
                            .where(
                                CatalogSyncLog.sync_id == uuid.UUID(sync_id),
                                CatalogSyncLog.status == "failed"
                            )
                            .order_by(CatalogSyncLog.created_at.asc())
                        )).scalars().all()
                        return JSONResponse(content={"success": True, "data": {
                            "sync_id":      sync_id,
                            "total_errors": len(logs),
                            "errors": [{
                                "id":           str(l.id),
                                "product_id":   l.product_id,
                                "name":         l.product_name,
                                "reason":       l.error,
                            } for l in logs],
                        }}, status_code=200)

                    # ── meta_disconnect — clear meta credentials ──────────────────────

                    case 'meta_disconnect':
                        row = await _get_row(db, body)
                        if not row:
                            raise Exception("record not found")
                        d   = dict(row.data or {})
                        cfg = dict(d.get('config', {}))
                        cfg['meta'] = {
                            'access_token': '', 'waba_id': '',
                            'phone_number_id': '', 'catalog_id': ''
                        }
                        d['config'] = cfg
                        row.data = d
                        await db.commit()
                        return JSONResponse(content={"success": True,
                            "message": "Seller connection records removed."}, status_code=200)
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
                        # pull customer names from whatsapp_customers for enrichment
                        all_phones = list({l.from_ for l in logs if l.from_})
                        cust_map = {}
                        if all_phones:
                            cust_rows = (await db.execute(
                                select(WhatsappCustomer)
                                .where(WhatsappCustomer.phone_number.in_(all_phones))
                            )).scalars().all()
                            cust_map = {c.phone_number: c.name for c in cust_rows}
                        contacts_map = {}
                        for l in logs:
                            phone = l.from_ or ''
                            if phone not in contacts_map:
                                contacts_map[phone] = {
                                    "phone":        phone,
                                    "name":         cust_map.get(phone, phone),
                                    "last_message": l.message,
                                    "last_seen":    l.created_at.isoformat() if l.created_at else None,
                                    "count":        0,
                                }
                            contacts_map[phone]['count'] += 1
                        contacts = sorted(
                            contacts_map.values(),
                            key=lambda x: x['count'],
                            reverse=True
                        )
                        total_received = sum(c['count'] for c in contacts)
                        total_sent = (await db.execute(
                            select(sa_func.count(WhatsappLog.id))
                            .where(WhatsappLog.direction == 'out')
                        )).scalar()
                        return JSONResponse(content={"success": True, "data": {
                            "contacts": contacts,
                            "stats": {
                                "total_contacts": len(contacts),
                                "total_received": total_received,
                                "total_sent":     total_sent,
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

                    # ── Analytics (req 3) ───────────────────────────────────

                    case 'analytics':
                        from sqlalchemy import cast, Date as SADate
                        now      = datetime.now(timezone.utc)
                        today    = now.date()
                        week_ago = now - timedelta(days=7)
                        month_ago = now - timedelta(days=30)

                        total_msgs_r   = await db.execute(sa_func.count(WhatsappMessage.id).select())
                        total_msgs     = (await db.execute(select(sa_func.count(WhatsappMessage.id)))).scalar()
                        total_cust     = (await db.execute(select(sa_func.count(WhatsappCustomer.id)))).scalar()
                        msgs_today     = (await db.execute(
                            select(sa_func.count(WhatsappMessage.id))
                            .where(cast(WhatsappMessage.timestamp, SADate) == today)
                        )).scalar()
                        msgs_week      = (await db.execute(
                            select(sa_func.count(WhatsappMessage.id))
                            .where(WhatsappMessage.timestamp >= week_ago)
                        )).scalar()
                        msgs_month     = (await db.execute(
                            select(sa_func.count(WhatsappMessage.id))
                            .where(WhatsappMessage.timestamp >= month_ago)
                        )).scalar()

                        # top customers by message count
                        top_cust_rows = (await db.execute(
                            select(WhatsappCustomer)
                            .order_by(WhatsappCustomer.total_messages.desc())
                            .limit(10)
                        )).scalars().all()
                        top_customers = [{
                            "id": str(c.id), "phone": c.phone_number,
                            "name": c.name, "total_messages": c.total_messages,
                            "last_seen": c.last_seen.isoformat() if c.last_seen else None,
                        } for c in top_cust_rows]

                        # most requested products (messages containing product keywords)
                        prod_rows = (await db.execute(
                            select(WhatsappMessage.message_text, sa_func.count(WhatsappMessage.id).label('cnt'))
                            .where(WhatsappMessage.direction == 'incoming')
                            .where(WhatsappMessage.message_text.ilike('%product%') |
                                   WhatsappMessage.message_text.ilike('%catalog%') |
                                   WhatsappMessage.message_text.ilike('%price%'))
                            .group_by(WhatsappMessage.message_text)
                            .order_by(sa_func.count(WhatsappMessage.id).desc())
                            .limit(10)
                        )).all()
                        most_requested = [{"query": r[0], "count": r[1]} for r in prod_rows]

                        # daily message counts (last 30 days)
                        from sqlalchemy import text as sa_text
                        daily_rows = (await db.execute(sa_text(
                            "SELECT DATE(timestamp) as day, COUNT(*) as cnt "
                            "FROM whatsapp_messages "
                            "WHERE timestamp >= NOW() - INTERVAL '30 days' "
                            "GROUP BY day ORDER BY day"
                        ))).all()
                        daily_counts = [{"date": str(r[0]), "count": r[1]} for r in daily_rows]

                        # customer growth (daily new customers last 30 days)
                        growth_rows = (await db.execute(sa_text(
                            "SELECT DATE(first_seen) as day, COUNT(*) as cnt "
                            "FROM whatsapp_customers "
                            "WHERE first_seen >= NOW() - INTERVAL '30 days' "
                            "GROUP BY day ORDER BY day"
                        ))).all()
                        customer_growth = [{"date": str(r[0]), "count": r[1]} for r in growth_rows]

                        return JSONResponse(content={"success": True, "data": {
                            "total_messages":   total_msgs,
                            "total_customers":  total_cust,
                            "messages_today":   msgs_today,
                            "messages_week":    msgs_week,
                            "messages_month":   msgs_month,
                            "top_customers":    top_customers,
                            "most_requested_products": most_requested,
                            "daily_message_counts":    daily_counts,
                            "customer_growth":         customer_growth,
                        }}, status_code=200)

                    # ── Customer list (req 4) ───────────────────────────────

                    case 'customers':
                        limit  = int(body.get('limit', 50))
                        offset = int(body.get('offset', 0))
                        rows   = (await db.execute(
                            select(WhatsappCustomer)
                            .order_by(WhatsappCustomer.last_seen.desc())
                            .limit(limit).offset(offset)
                        )).scalars().all()
                        total = (await db.execute(select(sa_func.count(WhatsappCustomer.id)))).scalar()
                        return JSONResponse(content={"success": True, "data": {
                            "total": total,
                            "customers": [{
                                "id":             str(c.id),
                                "wa_id":          c.wa_id,
                                "phone_number":   c.phone_number,
                                "name":           c.name,
                                "first_seen":     c.first_seen.isoformat() if c.first_seen else None,
                                "last_seen":      c.last_seen.isoformat() if c.last_seen else None,
                                "total_messages": c.total_messages,
                            } for c in rows],
                        }}, status_code=200)

                    # ── Customer detail (req 4) ─────────────────────────────

                    case 'customer_detail':
                        phone = body.get('phone') or body.get('phone_number')
                        cid   = body.get('customer_id')
                        if cid:
                            cust = (await db.execute(
                                select(WhatsappCustomer).where(WhatsappCustomer.id == uuid.UUID(cid))
                            )).scalar_one_or_none()
                        elif phone:
                            cust = (await db.execute(
                                select(WhatsappCustomer).where(WhatsappCustomer.phone_number == phone)
                            )).scalar_one_or_none()
                        else:
                            raise Exception("phone or customer_id required")
                        if not cust:
                            raise Exception("customer not found")
                        msgs = (await db.execute(
                            select(WhatsappMessage)
                            .where(WhatsappMessage.phone_number == cust.phone_number)
                            .order_by(WhatsappMessage.timestamp.asc())
                        )).scalars().all()
                        return JSONResponse(content={"success": True, "data": {
                            "customer": {
                                "id": str(cust.id), "wa_id": cust.wa_id,
                                "phone_number": cust.phone_number, "name": cust.name,
                                "first_seen": cust.first_seen.isoformat() if cust.first_seen else None,
                                "last_seen":  cust.last_seen.isoformat() if cust.last_seen else None,
                                "total_messages": cust.total_messages,
                            },
                            "messages": [{
                                "id":          str(m.id),
                                "wa_id":       m.wa_id,
                                "text":        m.message_text,
                                "type":        m.message_type,
                                "direction":   m.direction,
                                "timestamp":   m.timestamp.isoformat() if m.timestamp else None,
                            } for m in msgs],
                        }}, status_code=200)

                    # ── Latest messages (req 4) ─────────────────────────────

                    case 'latest_messages':
                        limit = int(body.get('limit', 20))
                        rows  = (await db.execute(
                            select(WhatsappMessage)
                            .order_by(WhatsappMessage.timestamp.desc())
                            .limit(limit)
                        )).scalars().all()
                        return JSONResponse(content={"success": True, "data": [{
                            "id":           str(m.id),
                            "wa_id":        m.wa_id,
                            "phone_number": m.phone_number,
                            "customer_name":m.customer_name,
                            "text":         m.message_text,
                            "type":         m.message_type,
                            "direction":    m.direction,
                            "timestamp":    m.timestamp.isoformat() if m.timestamp else None,
                        } for m in rows]}, status_code=200)

                    # ── message_log — all messages paginated (unblocks FragMessages) ──

                    case 'message_log':
                        limit  = int(body.get('limit', 100))
                        offset = int(body.get('offset', 0))
                        result = await db.execute(
                            select(WhatsappLog)
                            .where(WhatsappLog.direction.in_(['in', 'out']))
                            .order_by(WhatsappLog.created_at.desc())
                            .limit(limit).offset(offset)
                        )
                        logs = result.scalars().all()
                        return JSONResponse(content={"success": True, "data": [{
                            "id":         str(l.id),
                            "direction":  l.direction,
                            "from":       l.from_,
                            "to":         l.to_,
                            "message":    l.message,
                            "type":       (l.payload or {}).get('type', 'text') if l.payload else 'text',
                            "created_at": l.created_at.isoformat() if l.created_at else None,
                            "status":     l.msg_status or "sent",
                        } for l in logs]}, status_code=200)

                    # ── message_stats — accurate daily chart (unblocks FragAnalytics) ─

                    case 'message_stats':
                        from sqlalchemy import cast, Date as SADate, text as sa_text
                        now       = datetime.now(timezone.utc)
                        today     = now.date()
                        week_ago  = now - timedelta(days=7)
                        month_ago = now - timedelta(days=30)
                        msgs_today = (await db.execute(
                            select(sa_func.count(WhatsappLog.id))
                            .where(cast(WhatsappLog.created_at, SADate) == today)
                            .where(WhatsappLog.direction != 'event')
                        )).scalar()
                        msgs_week = (await db.execute(
                            select(sa_func.count(WhatsappLog.id))
                            .where(WhatsappLog.created_at >= week_ago)
                            .where(WhatsappLog.direction != 'event')
                        )).scalar()
                        msgs_month = (await db.execute(
                            select(sa_func.count(WhatsappLog.id))
                            .where(WhatsappLog.created_at >= month_ago)
                            .where(WhatsappLog.direction != 'event')
                        )).scalar()
                        daily_rows = (await db.execute(sa_text(
                            "SELECT DATE(created_at) as day, COUNT(*) as cnt "
                            "FROM whatsapp_log "
                            "WHERE created_at >= NOW() - INTERVAL '30 days' "
                            "AND direction != 'event' "
                            "GROUP BY day ORDER BY day ASC"
                        ))).all()
                        # build a full 30-day series with 0-fill so chart never has gaps
                        counts_by_date = {str(r[0]): r[1] for r in daily_rows}
                        daily = [
                            {"date": str(today - timedelta(days=i)), "count": counts_by_date.get(str(today - timedelta(days=i)), 0)}
                            for i in range(29, -1, -1)
                        ]
                        return JSONResponse(content={"success": True, "data": {
                            "today":      msgs_today,
                            "this_week":  msgs_week,
                            "this_month": msgs_month,
                            "daily":      daily,
                        }}, status_code=200)

                    # ── product_analytics — accurate product demand tracking ──────────

                    case 'product_analytics':
                        prod_rows = (await db.execute(
                            select(
                                WhatsappProductRequest.product_name,
                                sa_func.count(WhatsappProductRequest.id).label('cnt')
                            )
                            .group_by(WhatsappProductRequest.product_name)
                            .order_by(sa_func.count(WhatsappProductRequest.id).desc())
                            .limit(50)
                        )).all()
                        total_requests = (await db.execute(
                            select(sa_func.count(WhatsappProductRequest.id))
                        )).scalar()
                        unique_products = (await db.execute(
                            select(sa_func.count(sa_func.distinct(WhatsappProductRequest.product_name)))
                        )).scalar()
                        return JSONResponse(content={"success": True, "data": {
                            "top_products": [{"name": r[0], "count": r[1]} for r in prod_rows],
                            "total_product_requests":   total_requests,
                            "unique_products_requested": unique_products,
                        }}, status_code=200)

                    # ── Business profile (req 6) ────────────────────────────

                    case 'get_profile':
                        row = await _get_first_business_row(db)
                        if not row:
                            raise Exception("no business record found")
                        d = row.data or {}
                        return JSONResponse(content={"success": True, "data": {
                            "id":          str(row.id),
                            "profile":     d.get('profile', {}),
                            "title":       d.get('profile', {}).get('title', d.get('title', '')),
                            "description": d.get('profile', {}).get('description', ''),
                            "category":    d.get('profile', {}).get('category', ''),
                            "logo_url":    d.get('profile', {}).get('logo_url', ''),
                            "phone":       d.get('profile', {}).get('phone', ''),
                        }}, status_code=200)

                    case 'save_profile':
                        row = await _get_row(db, body)
                        if not row:
                            raise Exception("record not found")
                        d = dict(row.data or {})
                        d['profile'] = body.get('profile', body.get('data', {}))
                        row.data = d
                        await db.commit()
                        # push to Meta
                        warning  = None
                        meta     = _meta_cfg(row)
                        token    = meta.get('access_token')
                        phone_id = meta.get('phone_number_id')
                        if token and phone_id:
                            try:
                                sc, meta_resp = await _push_profile_to_meta(token, phone_id, d['profile'])
                                if sc != 200:
                                    warning = f"Saved but Meta push failed: {meta_resp}"
                            except Exception as e:
                                warning = f"Saved but Meta push failed: {e}"
                        resp = {"success": True, "data": {}}
                        if warning:
                            resp["warning"] = warning
                        return JSONResponse(content=resp, status_code=200)

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
        def _obj(**props):
            return {"type": "object", "additionalProperties": True, "properties": props}
        _schemas = {
            'create':               _obj(user_id={"type": "string"}, data={"type": "object"}),
            'get':                  _obj(id={"type": "string"}),
            'list':                 _obj(user_id={"type": "string"}),
            'update':               _obj(user_id={"type": "string"}, data={"type": "object"}),
            'delete':               _obj(id={"type": "string"}),
            'meta_config_save':     _obj(id={"type": "string"}, user_id={"type": "string"}, meta={"type": "object"}, data={"type": "object"}),
            'save_meta_config':     _obj(id={"type": "string"}, user_id={"type": "string"}, meta={"type": "object"}, data={"type": "object"}),
            'get_meta_config':      _obj(id={"type": "string"}, user_id={"type": "string"}),
            'meta_test':            _obj(id={"type": "string"}, user_id={"type": "string"}),
            'meta_sync_profile':    _obj(id={"type": "string"}, user_id={"type": "string"}),
            'meta_connect':         _obj(id={"type": "string"}, user_id={"type": "string"}, access_token={"type": "string"}),
            'catalog_sync':         _obj(id={"type": "string"}, user_id={"type": "string"}, access_token={"type": "string"}, base_url={"type": "string"}),
            'catalog_status':       _obj(id={"type": "string"}, user_id={"type": "string"}),
            'catalog_validate':     _obj(id={"type": "string"}, user_id={"type": "string"}),
            'catalog_details':      _obj(id={"type": "string"}, user_id={"type": "string"}),
            'catalog_sync_full':    _obj(id={"type": "string"}, user_id={"type": "string"}, trigger={"type": "string"}),
            'catalog_sync_product': _obj(id={"type": "string"}, user_id={"type": "string"}, product={"type": "object"}),
            'catalog_delete_product': _obj(id={"type": "string"}, user_id={"type": "string"}, retailer_id={"type": "string"}),
            'catalog_sync_history': _obj(id={"type": "string"}, user_id={"type": "string"}, limit={"type": "integer"}, offset={"type": "integer"}),
            'catalog_sync_status':  _obj(id={"type": "string"}, user_id={"type": "string"}),
            'catalog_sync_errors':  _obj(id={"type": "string"}, user_id={"type": "string"}, sync_id={"type": "string"}),
            'meta_disconnect':       _obj(id={"type": "string"}, user_id={"type": "string"}),
            'meta_oauth_start':       _obj(id={"type": "string"}, user_id={"type": "string"}, redirect_uri={"type": "string"}, state={"type": "string"}),
            'meta_oauth_callback':    _obj(id={"type": "string"}, user_id={"type": "string"}, code={"type": "string"}, redirect_uri={"type": "string"}),
            'meta_connection_status': _obj(id={"type": "string"}, user_id={"type": "string"}),
            'webhook_event':        _obj(),
            'get_automation_config':_obj(id={"type": "string"}, user_id={"type": "string"}),
            'save_automation_config':_obj(id={"type": "string"}, user_id={"type": "string"}, data={"type": "object"}),
            'conversation_list':    _obj(id={"type": "string"}, user_id={"type": "string"}),
            'conversation_messages':_obj(id={"type": "string"}, user_id={"type": "string"}, phone={"type": "string"}),
            'get_logs':             _obj(id={"type": "string"}, limit={"type": "integer"}),
            'send_message':         _obj(id={"type": "string"}, user_id={"type": "string"}, to={"type": "string"}, message={"type": "string"}),
            'analytics':            _obj(id={"type": "string"}, user_id={"type": "string"}),
            'customers':            _obj(id={"type": "string"}, user_id={"type": "string"}, limit={"type": "integer"}, offset={"type": "integer"}),
            'customer_detail':      _obj(id={"type": "string"}, user_id={"type": "string"}, phone={"type": "string"}, customer_id={"type": "string"}),
            'latest_messages':      _obj(id={"type": "string"}, limit={"type": "integer"}),
            'message_log':          _obj(id={"type": "string"}, user_id={"type": "string"}, limit={"type": "integer"}, offset={"type": "integer"}),
            'message_stats':        _obj(id={"type": "string"}, user_id={"type": "string"}),
            'product_analytics':    _obj(id={"type": "string"}, user_id={"type": "string"}),
            'get_profile':          _obj(id={"type": "string"}, user_id={"type": "string"}),
            'save_profile':         _obj(id={"type": "string"}, user_id={"type": "string"}, profile={"type": "object"}, data={"type": "object"}),
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
