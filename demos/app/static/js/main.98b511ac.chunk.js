(this["webpackJsonprecipe-contacts"]=this["webpackJsonprecipe-contacts"]||[]).push([[0],{15:function(e,t,n){},18:function(e,t,n){},2:function(e,t,n){e.exports={grid:"preview_styles_grid__1XMHs",row:"preview_styles_row__2fsI7",header:"preview_styles_header__32QiA",oddRow:"preview_styles_oddRow__k7ReU",evenRow:"preview_styles_evenRow__1nBTW",firstCell:"preview_styles_firstCell__1lsWo",lastCell:"preview_styles_lastCell__36OfD",rightAligned:"preview_styles_rightAligned__OARq-"}},20:function(e,t,n){"use strict";n.r(t);var a=n(0),c=n.n(a),r=n(7),s=n.n(r),o=(n(15),n(3)),i=n.n(o),u=n(4),d=n(5),l=n(2),w=n.n(l),f=n(1),h=function(e){var t=e.data.map((function(e,t){var n="".concat(0==t?"".concat(w.a.row," ").concat(w.a.header):""),a="".concat(w.a.row," ").concat(t%2==0?w.a.evenRow:w.a.oddRow);return e.map((function(c,r){var s,o="".concat(0==(s=r)?w.a.firstCell:""," ").concat(s==e.length-1?w.a.lastCell:""),i=function(e){return"".concat(e>=4?w.a.rightAligned:"")}(r),u="".concat(n," ").concat(a," ").concat(o," ").concat(i),d="".concat(n," ").concat(i," ").concat(o),l=0==t?d:u;return Object(f.jsx)("div",{style:{gridColumn:r+1},className:l,children:c},r)}))}));return Object(f.jsx)("div",{className:w.a.grid,children:t})},j=function(e){var t=e.data;return Object(f.jsxs)(c.a.Fragment,{children:[Object(f.jsxs)("p",{className:w.a.p,children:["The ",Object(f.jsx)("strong",{children:"Contacts"})," from the CRM has been synced! See a preview of the data below."]}),Object(f.jsx)("div",{style:{padding:"2rem"},children:Object(f.jsx)(h,{data:t})})]})},p=n(8),b=n(9),v=n.n(b),y=function(e){var t=e.side,n=t?Object(p.a)({},"margin".concat(t),"1rem"):{};return Object(f.jsxs)("div",{className:v.a["lds-ring"],style:n,children:[Object(f.jsx)("div",{}),Object(f.jsx)("div",{}),Object(f.jsx)("div",{}),Object(f.jsx)("div",{})]})},O=n(10),g=new URL(document.location).searchParams,x=g.get("apiKey"),_=g.get("recipeId"),m=g.get("envId"),k=g.get("tenantId"),C=g.get("flowId"),H=g.get("awsEndpoint");function R(){return G.apply(this,arguments)}function G(){return(G=Object(u.a)(i.a.mark((function e(){var t,n;return i.a.wrap((function(e){for(;;)switch(e.prev=e.next){case 0:return e.next=2,fetch(H);case 2:if((t=e.sent).ok){e.next=5;break}return e.abrupt("return",void 0);case 5:return e.next=7,t.json();case 7:return n=e.sent,e.abrupt("return",n);case 9:case"end":return e.stop()}}),e)})))).apply(this,arguments)}n(18),n(19);var S=function(){var e=c.a.useState(),t=Object(d.a)(e,2),n=t[0],a=t[1],r=c.a.useState(!1),s=Object(d.a)(r,2),o=s[0],l=s[1],w=c.a.useState(!1),h=Object(d.a)(w,2),p=h[0],b=h[1];c.a.useEffect((function(){window.HotGlue&&!window.HotGlue.hasMounted()&&window.HotGlue.mount({api_key:x,env_id:m})}),[window.HotGlue]);var v=function(){var e=Object(u.a)(i.a.mark((function e(){var t,n;return i.a.wrap((function(e){for(;;)switch(e.prev=e.next){case 0:return e.next=2,R();case 2:if(t=e.sent){e.next=5;break}return e.abrupt("return");case 5:n=[Object.keys(t[0])],t.forEach((function(e){var t=Object.values(e).map((function(e){return Object(e)===e?JSON.stringify(e):e}));n.push(t)})),a(n);case 8:case"end":return e.stop()}}),e)})));return function(){return e.apply(this,arguments)}}();c.a.useEffect((function(){n||v()}),[n]);var g=function e(){window.HotGlue&&window.HotGlue.hasMounted()?window.HotGlue.getLinkedFlows(k).then((function(e){e&&e.find((function(e){return e.id===C}))&&b(!0)})):setTimeout((function(){return e()}),1e3)};c.a.useEffect((function(){p||g()}),[p]);var H=function(){var e=Object(u.a)(i.a.mark((function e(){return i.a.wrap((function(e){for(;;)switch(e.prev=e.next){case 0:if(window.HotGlue&&window.HotGlue.hasMounted()){e.next=2;break}return e.abrupt("return");case 2:l(!0),window.HotGlue.createJob(C,k).then((function(e){window.swal("Syncing data","Starting a data sync. This may take a few minutes!","success"),window.HotGlue.pollJob(e.s3_root,C,k).then((function(e){var t=e.payload;l(!1),"JOB_COMPLETED"===t.status?(window.swal("Data synced","Contacts data has been synced successfully!","success"),v()):window.swal("Failed to sync","There was an issue syncing the data, please contact support for help.","error")}))}));case 4:case"end":return e.stop()}}),e)})));return function(){return e.apply(this,arguments)}}(),G=function(){var e=Object(u.a)(i.a.mark((function e(t,n){return i.a.wrap((function(e){for(;;)switch(e.prev=e.next){case 0:b(!0),window.swal("".concat(t.label," linked"),"Woohoo! You've linked a source! Now you can sync your data.","success");case 2:case"end":return e.stop()}}),e)})));return function(t,n){return e.apply(this,arguments)}}();return Object(f.jsxs)("div",{className:"container",children:[Object(f.jsx)("h1",{children:"hotglue Contacts recipe"}),Object(f.jsxs)("p",{children:["This is a React app showing an end-to-end sample of a hotglue-powered integration that pulls ",Object(f.jsx)("strong",{children:_})," data. Follow each step below to see a user experience."]}),Object(f.jsx)("h2",{children:"Connect your data"}),Object(f.jsxs)("p",{children:["Start by connecting your ",_," data below! ",Object(f.jsx)("a",{href:"#",children:"I don't have an account."})]}),Object(f.jsx)(O.a,{tenant:k,onLink:G}),Object(f.jsx)("h2",{children:"Trigger a job"}),p?Object(f.jsxs)("p",{children:["Now that our ",_," data is linked, the user can sync their data. You can also set a schedule to automatically sync new data."]}):Object(f.jsxs)("p",{children:["Once you link your ",_," data, you can sync it!"]}),Object(f.jsx)("div",{className:"button",children:Object(f.jsxs)("a",{style:{color:"#ffffff"},className:"btnForward ".concat(!p&&"disabled"),onClick:H,children:["Sync data",o&&Object(f.jsx)(y,{side:"Left"})]})}),Object(f.jsx)("h2",{children:"Preview your data"}),n?Object(f.jsx)(j,{data:n}):Object(f.jsxs)("p",{children:["Once you connect your ",_," data and run a sync job, data will appear here!"]})]})},N=function(e){e&&e instanceof Function&&n.e(3).then(n.bind(null,21)).then((function(t){var n=t.getCLS,a=t.getFID,c=t.getFCP,r=t.getLCP,s=t.getTTFB;n(e),a(e),c(e),r(e),s(e)}))};s.a.render(Object(f.jsx)(c.a.StrictMode,{children:Object(f.jsx)(S,{})}),document.getElementById("root")),N()},9:function(e,t,n){e.exports={"lds-ring":"loading_styles_lds-ring__YRoef"}}},[[20,1,2]]]);
//# sourceMappingURL=main.98b511ac.chunk.js.map