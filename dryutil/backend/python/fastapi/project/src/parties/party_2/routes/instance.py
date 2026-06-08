from fastapi import APIRouter, Body, Query, Path, Depends, Request
from fastapi.responses import JSONResponse, HTMLResponse, PlainTextResponse
#from my_ai_project.services.model import predict
#from src.parties.party_2.schema.instance import body
from src.parties.party_2.controllers.instance import index as controller_index
#set..
from src.shared.util.include_file.index import include_file;
from src.db_config import get_db
#from uuid import UUID
import uuid



#set..
_ins = {
    "router": {
       "public": APIRouter(),
       "private": APIRouter(), #APIRouter(dependencies=[Depends(AuthMiddleware)]),
    }
}



#set..
def index(_p={}):



    #set..
    router = _ins["router"]["public"]
    router_private = _ins["router"]["private"]

    #set..
    #_ep_prefix_0 = "/api/instance"
    _ep_prefix_0 = "/api/i";
    _ep_prefix_1 = "/api/i_init";
    #set..
    _ep_prefix_2 = "/api/doc";
    _ep_prefix_3 = "/api/doc-ui";


    
    #set..
    @router.post(_ep_prefix_0+'/test-public')
    async def run(
        #version: str = Path(..., regex=PATH_SCHEMA["properties"]["version"]["pattern"], description="API version"),
        #body: dict = Body(..., embed=False, schema_extra=BODY_SCHEMA),
        #verbose: bool = Query(QUERY_SCHEMA["properties"]["verbose"]["default"], description="Verbose output")
    ):
        #return controller_index()[0]()
        #create = controller_index()  # get inner functions
        #_rsp = create;
        #return  _rsp;
        _func, _ = await controller_index()   # unpack functions
        return await _func()       # call create
    


    
 



    #============PRIVATE==========# [START]
    #set..
    @router_private.api_route(_ep_prefix_0+'/'+"{project}"+'/'+"{instance}", methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"])
    async def run(
        request: Request,
        project: str,
        instance: str,
        #version: str = Path(..., regex=PATH_SCHEMA["properties"]["version"]["pattern"], description="API version"),
        #body: dict = Body(..., embed=False, schema_extra=BODY_SCHEMA),
        body: dict = Depends( include_file("src/parties/party_2/schema/instance.py", lambda name, module: ())[0][1].body ),
        #verbose: bool = Query(QUERY_SCHEMA["properties"]["verbose"]["default"], description="Verbose output")
        db=Depends(get_db)
    ):

        #return controller_index()[0]()
        #create = controller_index()  # get inner functions
        #_rsp = create;
        #return  _rsp;
        _func, _, __ = await controller_index()   # unpack functions
        return await _func(request,project,instance,body,db)       # call create
    """
        return JSONResponse(
                content={"status": "success", "output": {"id": "-"}},
                status_code=201,
        )
    """



    #set..
    @router_private.api_route(_ep_prefix_2+'/'+"{project}"+'/'+"{instance}", methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"])
    async def run(
        request: Request,
        project: str,
        instance: str,
        #version: str = Path(..., regex=PATH_SCHEMA["properties"]["version"]["pattern"], description="API version"),
        #body: dict = Body(..., embed=False, schema_extra=BODY_SCHEMA),
        #body: dict = Depends( include_file("src/parties/party_2/schema/instance.py", lambda name, module: ())[0][1].body ),
        #verbose: bool = Query(QUERY_SCHEMA["properties"]["verbose"]["default"], description="Verbose output")
        db=Depends(get_db)
    ):

        #return controller_index()[0]()
        #create = controller_index()  # get inner functions
        #_rsp = create;
        #return  _rsp;
        body = None
        __, _, _func  = await controller_index()   # unpack functions
        return await _func(request,project,instance,body,db)       # call create
    """
        return JSONResponse(
                content={"status": "success", "output": {"id": "-"}},
                status_code=201,
        )
    """
    
    #============PRIVATE==========# [END]






    #============PUBLIC==========# [START]

    # -- WhatsApp Webhook (Meta calls this — no JWT required) --
    @router.get("/api/webhook/{project}/{instance}")
    async def webhook_verify(
        request: Request,
        project: str,
        instance: str,
    ):
        params       = dict(request.query_params)
        mode         = params.get("hub.mode", "")
        challenge    = params.get("hub.challenge", "")
        verify_token = params.get("hub.verify_token", "")
        print(f"[webhook/verify] mode={mode} verify_token={verify_token} challenge={challenge}")
        if mode == "subscribe" and verify_token == "my_verify_token_123" and challenge:
            return PlainTextResponse(challenge, status_code=200)
        return PlainTextResponse("Forbidden", status_code=403)

    @router.post("/api/webhook/{project}/{instance}")
    async def webhook_receive(
        request: Request,
        project: str,
        instance: str,
        db=Depends(get_db)
    ):
        from src.parties.party_2.controllers.instance import index as controller_index
        _func, _, __ = await controller_index()
        # reuse i() with a fake typ=webhook_event injected via query
        # We pass the raw webhook payload directly to the controller
        # controller will call utility 701's i() with typ=webhook_event
        from starlette.datastructures import QueryParams
        # Patch query params to inject typ
        scope = dict(request.scope)
        scope["query_string"] = b"typ=webhook_event"
        from starlette.requests import Request as StarletteRequest
        patched = StarletteRequest(scope, request.receive)
        return await _func(patched, project, instance, None, db)

    @router.get(_ep_prefix_3+'/'+"{project}"+'/'+"{instance}", include_in_schema=False)
    async def swagger_ui():
      #print("--swagger_ui")
      _dta = {
          "suffix": uuid.uuid4(),
      }
      html = f"""
    <!DOCTYPE html>
    <html>
    <head>
      <title>Dynamic Swagger Docs</title>
      <link rel="stylesheet" type="text/css"
            href="https://unpkg.com/swagger-ui-dist/swagger-ui.css" />
    </head>
    <body>
      <div id="swagger-ui"></div>
      <div id="form-ui-{_dta['suffix']}" >
        <div > Access token </div>
        <div style="height:1vh;" > </div>
        <input id="a-token-ui-{_dta['suffix']}" type="text" placeholder="paste your access-token.." />
        <div style="height:1vh;" > </div>
        <button id="update-ui-{_dta['suffix']}" >Update</button>
        <div style="height:1vh;" > </div>
        <div > <b>NOTE:</b> Access token is required to view Api Docs </div>
      </div>
      <script src="https://unpkg.com/swagger-ui-dist/swagger-ui-bundle.js"></script>
      <script>
        let _d = {{
          "access_token": localStorage.getItem('access_token'),
          "u_arr": location.pathname.split('/')
        }};
        //alert(JSON.stringify(_dta))
        //console.log(_d.u_arr);
        let mE_f = document.getElementById("form-ui-{_dta['suffix']}");
        let mE_a = document.getElementById("a-token-ui-{_dta['suffix']}");
        let mE_b = document.getElementById("update-ui-{_dta['suffix']}");
        //set..
        mE_b.onclick = () => {{
        const _v = mE_a.value.trim();
        //console.log(_v);
        localStorage.setItem('access_token', _v);
        //refresh..
        location.href = location.href;
        }};
        //check & set..
        if (_d[`access_token`]){{
        /*SwaggerUIBundle({{
          url: `/client/api/doc/${{_d['u_arr'][4]}}/${{_d['u_arr'][5]}}`, //'/client/api/doc/ona/vibe_coding', //'/swagger.json',
          dom_id: '#swagger-ui',
          presets: [SwaggerUIBundle.presets.apis, SwaggerUIBundle.SwaggerUIStandalonePreset],
        }});
        */

        fetch(`/client/api/doc/${{_d['u_arr'][4]}}/${{_d['u_arr'][5]}}`, {{
  headers: {{
    "Authorization": `Bearer ${{_d[`access_token`]}}`,
  }},
}})
  .then(res => res.json())
  .then(spec => {{
    SwaggerUIBundle({{
      spec,  // use the loaded spec directly
      dom_id: "#swagger-ui",
      deepLinking: true,
      presets: [
        SwaggerUIBundle.presets.apis,
        SwaggerUIBundle.SwaggerUIStandalonePreset
      ],
      layout: "BaseLayout",
      persistAuthorization: true,
      requestInterceptor: (req) => {{
        req.headers["Authorization"] = `Bearer ${{_d[`access_token`]}}`;
        return req;
      }}
    }});
  }})
  .catch(err => {{
    console.error("Failed to load Swagger spec:", err);
    document.body.innerHTML = "<p>❌ Unauthorized or failed to load swagger.json</p>";
  }});

        }}else{{
        //mE_f.style.display = "block";
        }}

      </script>
    </body>
    </html>
    """
      return HTMLResponse(html)

    #============PUBLIC==========# [END]




    #============DASHBOARD (WhatsApp CRM)==========# [START]
    # All routes hit the 701 utility via a lightweight internal helper.
    # They are PUBLIC (no JWT) so the frontend dashboard can call them directly.
    # Route prefix: /api/wa/

    async def _wa_call(project: str, instance: str, typ: str, body: dict, db):
        """Internal helper: build a fake request and dispatch to the 701 utility."""
        from starlette.datastructures import QueryParams
        from src.shared.utility.index import index as utility_index
        from src.shared.utility.u.fake_req_obj.index import fake_req_obj
        req = fake_req_obj(
            method="POST", url="",
            headers={"content-type": "application/json"},
            query_params={"typ": typ},
            path_params={},
            json_data=body,
            state={},
        )
        i, _, __, ___, ____ = await utility_index({'data': {}})
        # resolve the instance so engine is initialised
        from sqlalchemy.future import select
        from sqlalchemy.orm import joinedload
        from src.database.entity.instance import Instance
        from src.database.entity.project import Project
        result = await db.execute(
            select(Instance)
            .join(Project, Instance.project_id == Project.id)
            .where(Instance.name == instance, Project.name == project)
            .options(joinedload(Instance.project), joinedload(Instance.utility))
        )
        inst = result.scalar_one_or_none()
        if not inst:
            return JSONResponse(content={"success": False, "message": "instance not found"}, status_code=404)
        from src.shared.util.include_file.index import include_file
        lib_name, _lib_ = include_file(
            f"src/shared/utility/l/{inst.utility_id}/index.py", lambda n, m: ()
        )[0]
        fn_i, _, __, ___, ____ = await _lib_.index({'data': {'instance': inst}})
        return await fn_i(req)

    @router.get("/api/wa/{project}/{instance}/analytics")
    async def wa_analytics(project: str, instance: str, db=Depends(get_db)):
        return await _wa_call(project, instance, "analytics", {}, db)

    @router.get("/api/wa/{project}/{instance}/customers")
    async def wa_customers(
        project: str, instance: str,
        limit: int = 50, offset: int = 0,
        db=Depends(get_db)
    ):
        return await _wa_call(project, instance, "customers", {"limit": limit, "offset": offset}, db)

    @router.get("/api/wa/{project}/{instance}/customer/{phone}")
    async def wa_customer_detail(project: str, instance: str, phone: str, db=Depends(get_db)):
        return await _wa_call(project, instance, "customer_detail", {"phone": phone}, db)

    @router.get("/api/wa/{project}/{instance}/messages")
    async def wa_messages(project: str, instance: str, limit: int = 20, db=Depends(get_db)):
        return await _wa_call(project, instance, "latest_messages", {"limit": limit}, db)

    @router.get("/api/wa/{project}/{instance}/business-profile")
    async def wa_get_profile(project: str, instance: str, db=Depends(get_db)):
        return await _wa_call(project, instance, "get_profile", {}, db)

    @router.post("/api/wa/{project}/{instance}/business-profile")
    async def wa_save_profile(request: Request, project: str, instance: str, db=Depends(get_db)):
        body = await request.json() if request.headers.get("content-type") == "application/json" else {}
        return await _wa_call(project, instance, "save_profile", body, db)

    @router.get("/api/wa/{project}/{instance}/meta-config")
    async def wa_get_meta_config(project: str, instance: str, db=Depends(get_db)):
        return await _wa_call(project, instance, "get_meta_config", {}, db)

    @router.post("/api/wa/{project}/{instance}/meta-config")
    async def wa_save_meta_config(request: Request, project: str, instance: str, db=Depends(get_db)):
        body = await request.json() if request.headers.get("content-type") == "application/json" else {}
        if "id" not in body:
            body["id"] = "a25c09e8-9eae-4f27-8bdb-cada50482587"
        return await _wa_call(project, instance, "meta_config_save", body, db)

    @router.post("/api/wa/{project}/{instance}/restore-meta-config")
    async def wa_restore_meta_config(request: Request, project: str, instance: str, db=Depends(get_db)):
        """Public route to restore permanent meta credentials — no JWT needed."""
        body = await request.json() if request.headers.get("content-type") == "application/json" else {}
        body["id"] = "a25c09e8-9eae-4f27-8bdb-cada50482587"
        return await _wa_call(project, instance, "meta_config_save", body, db)

    #============DASHBOARD (WhatsApp CRM)==========# [END]


    #============META OAUTH CALLBACK==============# [START]
    # Meta redirects the seller's browser to this GET route after they grant permissions.
    # No JWT required — this is a browser redirect from Meta.

    @router.get("/api/meta/oauth/callback")
    async def meta_oauth_callback(
        request: Request,
        db=Depends(get_db)
    ):
        params   = dict(request.query_params)
        code     = params.get("code", "")
        state    = params.get("state", "")  # business_id passed as state
        error    = params.get("error", "")
        # frontend_url: where to redirect after OAuth — stored in state or config
        # state format: "<business_id>|<frontend_redirect_url>"
        frontend_redirect = "http://localhost:5173/fragmetaconnect"
        business_id = state
        if "|" in state:
            business_id, frontend_redirect = state.split("|", 1)

        if error:
            from fastapi.responses import RedirectResponse
            return RedirectResponse(
                url=f"{frontend_redirect}?error={error}",
                status_code=302
            )

        if not code or not business_id:
            from fastapi.responses import RedirectResponse
            return RedirectResponse(
                url=f"{frontend_redirect}?error=invalid_callback",
                status_code=302
            )

        try:
            from src.shared.util.include_file.index import include_file
            from sqlalchemy.future import select as sa_select
            from sqlalchemy.orm import joinedload
            from src.database.entity.instance import Instance
            from src.database.entity.project import Project
            from src.shared.utility.u.fake_req_obj.index import fake_req_obj
            from fastapi.responses import RedirectResponse

            result = await db.execute(
                sa_select(Instance)
                .join(Project, Instance.project_id == Project.id)
                .where(Instance.name == "s_whatsapp_business_mgmt", Project.name == "ona")
                .options(joinedload(Instance.project), joinedload(Instance.utility))
            )
            inst = result.scalar_one_or_none()
            if not inst:
                return RedirectResponse(url=f"{frontend_redirect}?error=instance_not_found", status_code=302)

            lib_name, _lib_ = include_file(
                f"src/shared/utility/l/{inst.utility_id}/index.py", lambda n, m: ()
            )[0]
            fn_i, _, __, ___, ____ = await _lib_.index({'data': {'instance': inst}})

            # redirect_uri must exactly match what was used in meta_oauth_start
            redirect_uri = str(request.url).split("?")[0]
            req = fake_req_obj(
                method="POST", url="",
                headers={"content-type": "application/json"},
                query_params={"typ": "meta_oauth_callback"},
                path_params={},
                json_data={"id": business_id, "code": code, "redirect_uri": redirect_uri},
                state={},
            )
            await fn_i(req)
            return RedirectResponse(url=f"{frontend_redirect}?connected=true", status_code=302)

        except Exception as e:
            from fastapi.responses import RedirectResponse
            return RedirectResponse(
                url=f"{frontend_redirect}?error=callback_failed",
                status_code=302
            )

    #============META OAUTH CALLBACK==============# [END]













     
    

    














