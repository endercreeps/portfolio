var TM=Object.defineProperty,wM=Object.defineProperties;var CM=Object.getOwnPropertyDescriptors;var Og=Object.getOwnPropertySymbols;var IM=Object.prototype.hasOwnProperty,AM=Object.prototype.propertyIsEnumerable;var Fg=(n,e,t)=>e in n?TM(n,e,{enumerable:!0,configurable:!0,writable:!0,value:t}):n[e]=t,It=(n,e)=>{for(var t in e||={})IM.call(e,t)&&Fg(n,t,e[t]);if(Og)for(var t of Og(e))AM.call(e,t)&&Fg(n,t,e[t]);return n},Gt=(n,e)=>wM(n,CM(e));var rn=null,cc=!1,rf=1,DM=null,yn=Symbol("SIGNAL");function Ae(n){let e=rn;return rn=n,e}function uc(){return rn}var Ns={version:0,lastCleanEpoch:0,dirty:!1,producers:void 0,producersTail:void 0,consumers:void 0,consumersTail:void 0,recomputing:!1,consumerAllowSignalWrites:!1,consumerIsAlwaysLive:!1,kind:"unknown",producerMustRecompute:()=>!1,producerRecomputeValue:()=>{},consumerMarkedDirty:()=>{},consumerOnSignalRead:()=>{}};function Lo(n){if(cc)throw new Error("");if(rn===null)return;rn.consumerOnSignalRead(n);let e=rn.producersTail;if(e!==void 0&&e.producer===n)return;let t,i=rn.recomputing;if(i&&(t=e!==void 0?e.nextProducer:rn.producers,t!==void 0&&t.producer===n)){rn.producersTail=t,t.lastReadVersion=n.version;return}let r=n.consumersTail;if(r!==void 0&&r.consumer===rn&&(!i||NM(r,rn)))return;let s=Ls(rn),o={producer:n,consumer:rn,nextProducer:t,prevConsumer:r,lastReadVersion:n.version,nextConsumer:void 0};rn.producersTail=o,e!==void 0?e.nextProducer=o:rn.producers=o,s&&Vg(n,o)}function kg(){rf++}function sf(n){if(!(Ls(n)&&!n.dirty)&&!(!n.dirty&&n.lastCleanEpoch===rf)){if(!n.producerMustRecompute(n)&&!Fo(n)){nf(n);return}n.producerRecomputeValue(n),nf(n)}}function of(n){if(n.consumers===void 0)return;let e=cc;cc=!0;try{for(let t=n.consumers;t!==void 0;t=t.nextConsumer){let i=t.consumer;i.dirty||RM(i)}}finally{cc=e}}function af(){return rn?.consumerAllowSignalWrites!==!1}function RM(n){n.dirty=!0,of(n),n.consumerMarkedDirty?.(n)}function nf(n){n.dirty=!1,n.lastCleanEpoch=rf}function Ps(n){return n&&Ug(n),Ae(n)}function Ug(n){n.producersTail=void 0,n.recomputing=!0}function Oo(n,e){Ae(e),n&&Bg(n)}function Bg(n){n.recomputing=!1;let e=n.producersTail,t=e!==void 0?e.nextProducer:n.producers;if(t!==void 0){if(Ls(n))do t=cf(t);while(t!==void 0);e!==void 0?e.nextProducer=void 0:n.producers=void 0}}function Fo(n){for(let e=n.producers;e!==void 0;e=e.nextProducer){let t=e.producer,i=e.lastReadVersion;if(i!==t.version||(sf(t),i!==t.version))return!0}return!1}function Vr(n){if(Ls(n)){let e=n.producers;for(;e!==void 0;)e=cf(e)}n.producers=void 0,n.producersTail=void 0,n.consumers=void 0,n.consumersTail=void 0}function Vg(n,e){let t=n.consumersTail,i=Ls(n);if(t!==void 0?(e.nextConsumer=t.nextConsumer,t.nextConsumer=e):(e.nextConsumer=void 0,n.consumers=e),e.prevConsumer=t,n.consumersTail=e,!i)for(let r=n.producers;r!==void 0;r=r.nextProducer)Vg(r.producer,r)}function cf(n){let e=n.producer,t=n.nextProducer,i=n.nextConsumer,r=n.prevConsumer;if(n.nextConsumer=void 0,n.prevConsumer=void 0,i!==void 0?i.prevConsumer=r:e.consumersTail=r,r!==void 0)r.nextConsumer=i;else if(e.consumers=i,!Ls(e)){let s=e.producers;for(;s!==void 0;)s=cf(s)}return t}function Ls(n){return n.consumerIsAlwaysLive||n.consumers!==void 0}function lf(n){DM?.(n)}function NM(n,e){let t=e.producersTail;if(t!==void 0){let i=e.producers;do{if(i===n)return!0;if(i===t)break;i=i.nextProducer}while(i!==void 0)}return!1}function uf(n,e){return Object.is(n,e)}function dc(n,e){let t=Object.create(PM);t.computation=n,e!==void 0&&(t.equal=e);let i=()=>{if(sf(t),Lo(t),t.value===lc)throw t.error;return t.value};return i[yn]=t,lf(t),i}var ef=Symbol("UNSET"),tf=Symbol("COMPUTING"),lc=Symbol("ERRORED"),PM=Gt(It({},Ns),{value:ef,dirty:!0,error:null,equal:uf,kind:"computed",producerMustRecompute(n){return n.value===ef||n.value===tf},producerRecomputeValue(n){if(n.value===tf)throw new Error("");let e=n.value;n.value=tf;let t=Ps(n),i,r=!1;try{i=n.computation(),Ae(null),r=e!==ef&&e!==lc&&i!==lc&&n.equal(e,i)}catch(s){i=lc,n.error=s}finally{Oo(n,t)}if(r){n.value=e;return}n.value=i,n.version++}});function LM(){throw new Error}var Hg=LM;function zg(n){Hg(n)}function df(n){Hg=n}var OM=null;function ff(n,e){let t=Object.create(fc);t.value=n,e!==void 0&&(t.equal=e);let i=()=>Gg(t);return i[yn]=t,lf(t),[i,o=>ko(t,o),o=>Wg(t,o)]}function Gg(n){return Lo(n),n.value}function ko(n,e){af()||zg(n),n.equal(n.value,e)||(n.value=e,FM(n))}function Wg(n,e){af()||zg(n),ko(n,e(n.value))}var fc=Gt(It({},Ns),{equal:uf,value:void 0,kind:"signal"});function FM(n){n.version++,kg(),of(n),OM?.(n)}var hf=Gt(It({},Ns),{consumerIsAlwaysLive:!0,consumerAllowSignalWrites:!0,dirty:!0,kind:"effect"});function pf(n){if(n.dirty=!1,n.version>0&&!Fo(n))return;n.version++;let e=Ps(n);try{n.cleanup(),n.fn()}finally{Oo(n,e)}}function _n(n){return typeof n=="function"}function hc(n){let t=n(i=>{Error.call(i),i.stack=new Error().stack});return t.prototype=Object.create(Error.prototype),t.prototype.constructor=t,t}var pc=hc(n=>function(t){n(this),this.message=t?`${t.length} errors occurred during unsubscription:
${t.map((i,r)=>`${r+1}) ${i.toString()}`).join(`
  `)}`:"",this.name="UnsubscriptionError",this.errors=t});function Uo(n,e){if(n){let t=n.indexOf(e);0<=t&&n.splice(t,1)}}var mn=class n{constructor(e){this.initialTeardown=e,this.closed=!1,this._parentage=null,this._finalizers=null}unsubscribe(){let e;if(!this.closed){this.closed=!0;let{_parentage:t}=this;if(t)if(this._parentage=null,Array.isArray(t))for(let s of t)s.remove(this);else t.remove(this);let{initialTeardown:i}=this;if(_n(i))try{i()}catch(s){e=s instanceof pc?s.errors:[s]}let{_finalizers:r}=this;if(r){this._finalizers=null;for(let s of r)try{jg(s)}catch(o){e=e??[],o instanceof pc?e=[...e,...o.errors]:e.push(o)}}if(e)throw new pc(e)}}add(e){var t;if(e&&e!==this)if(this.closed)jg(e);else{if(e instanceof n){if(e.closed||e._hasParent(this))return;e._addParent(this)}(this._finalizers=(t=this._finalizers)!==null&&t!==void 0?t:[]).push(e)}}_hasParent(e){let{_parentage:t}=this;return t===e||Array.isArray(t)&&t.includes(e)}_addParent(e){let{_parentage:t}=this;this._parentage=Array.isArray(t)?(t.push(e),t):t?[t,e]:e}_removeParent(e){let{_parentage:t}=this;t===e?this._parentage=null:Array.isArray(t)&&Uo(t,e)}remove(e){let{_finalizers:t}=this;t&&Uo(t,e),e instanceof n&&e._removeParent(this)}};mn.EMPTY=(()=>{let n=new mn;return n.closed=!0,n})();var mf=mn.EMPTY;function mc(n){return n instanceof mn||n&&"closed"in n&&_n(n.remove)&&_n(n.add)&&_n(n.unsubscribe)}function jg(n){_n(n)?n():n.unsubscribe()}var Jn={onUnhandledError:null,onStoppedNotification:null,Promise:void 0,useDeprecatedSynchronousErrorHandling:!1,useDeprecatedNextContext:!1};var Os={setTimeout(n,e,...t){let{delegate:i}=Os;return i?.setTimeout?i.setTimeout(n,e,...t):setTimeout(n,e,...t)},clearTimeout(n){let{delegate:e}=Os;return(e?.clearTimeout||clearTimeout)(n)},delegate:void 0};function $g(n){Os.setTimeout(()=>{let{onUnhandledError:e}=Jn;if(e)e(n);else throw n})}function gf(){}var qg=vf("C",void 0,void 0);function Xg(n){return vf("E",void 0,n)}function Yg(n){return vf("N",n,void 0)}function vf(n,e,t){return{kind:n,value:e,error:t}}var Hr=null;function Fs(n){if(Jn.useDeprecatedSynchronousErrorHandling){let e=!Hr;if(e&&(Hr={errorThrown:!1,error:null}),n(),e){let{errorThrown:t,error:i}=Hr;if(Hr=null,t)throw i}}else n()}function Zg(n){Jn.useDeprecatedSynchronousErrorHandling&&Hr&&(Hr.errorThrown=!0,Hr.error=n)}var zr=class extends mn{constructor(e){super(),this.isStopped=!1,e?(this.destination=e,mc(e)&&e.add(this)):this.destination=BM}static create(e,t,i){return new ks(e,t,i)}next(e){this.isStopped?_f(Yg(e),this):this._next(e)}error(e){this.isStopped?_f(Xg(e),this):(this.isStopped=!0,this._error(e))}complete(){this.isStopped?_f(qg,this):(this.isStopped=!0,this._complete())}unsubscribe(){this.closed||(this.isStopped=!0,super.unsubscribe(),this.destination=null)}_next(e){this.destination.next(e)}_error(e){try{this.destination.error(e)}finally{this.unsubscribe()}}_complete(){try{this.destination.complete()}finally{this.unsubscribe()}}},kM=Function.prototype.bind;function yf(n,e){return kM.call(n,e)}var xf=class{constructor(e){this.partialObserver=e}next(e){let{partialObserver:t}=this;if(t.next)try{t.next(e)}catch(i){gc(i)}}error(e){let{partialObserver:t}=this;if(t.error)try{t.error(e)}catch(i){gc(i)}else gc(e)}complete(){let{partialObserver:e}=this;if(e.complete)try{e.complete()}catch(t){gc(t)}}},ks=class extends zr{constructor(e,t,i){super();let r;if(_n(e)||!e)r={next:e??void 0,error:t??void 0,complete:i??void 0};else{let s;this&&Jn.useDeprecatedNextContext?(s=Object.create(e),s.unsubscribe=()=>this.unsubscribe(),r={next:e.next&&yf(e.next,s),error:e.error&&yf(e.error,s),complete:e.complete&&yf(e.complete,s)}):r=e}this.destination=new xf(r)}};function gc(n){Jn.useDeprecatedSynchronousErrorHandling?Zg(n):$g(n)}function UM(n){throw n}function _f(n,e){let{onStoppedNotification:t}=Jn;t&&Os.setTimeout(()=>t(n,e))}var BM={closed:!0,next:gf,error:UM,complete:gf};var Jg=typeof Symbol=="function"&&Symbol.observable||"@@observable";function Kg(n){return n}function Qg(n){return n.length===0?Kg:n.length===1?n[0]:function(t){return n.reduce((i,r)=>r(i),t)}}var Us=(()=>{class n{constructor(t){t&&(this._subscribe=t)}lift(t){let i=new n;return i.source=this,i.operator=t,i}subscribe(t,i,r){let s=HM(t)?t:new ks(t,i,r);return Fs(()=>{let{operator:o,source:a}=this;s.add(o?o.call(s,a):a?this._subscribe(s):this._trySubscribe(s))}),s}_trySubscribe(t){try{return this._subscribe(t)}catch(i){t.error(i)}}forEach(t,i){return i=ev(i),new i((r,s)=>{let o=new ks({next:a=>{try{t(a)}catch(c){s(c),o.unsubscribe()}},error:s,complete:r});this.subscribe(o)})}_subscribe(t){var i;return(i=this.source)===null||i===void 0?void 0:i.subscribe(t)}[Jg](){return this}pipe(...t){return Qg(t)(this)}toPromise(t){return t=ev(t),new t((i,r)=>{let s;this.subscribe(o=>s=o,o=>r(o),()=>i(s))})}}return n.create=e=>new n(e),n})();function ev(n){var e;return(e=n??Jn.Promise)!==null&&e!==void 0?e:Promise}function VM(n){return n&&_n(n.next)&&_n(n.error)&&_n(n.complete)}function HM(n){return n&&n instanceof zr||VM(n)&&mc(n)}function zM(n){return _n(n?.lift)}function tv(n){return e=>{if(zM(e))return e.lift(function(t){try{return n(t,this)}catch(i){this.error(i)}});throw new TypeError("Unable to lift unknown Observable type")}}function nv(n,e,t,i,r){return new Mf(n,e,t,i,r)}var Mf=class extends zr{constructor(e,t,i,r,s,o){super(e),this.onFinalize=s,this.shouldUnsubscribe=o,this._next=t?function(a){try{t(a)}catch(c){e.error(c)}}:super._next,this._error=r?function(a){try{r(a)}catch(c){e.error(c)}finally{this.unsubscribe()}}:super._error,this._complete=i?function(){try{i()}catch(a){e.error(a)}finally{this.unsubscribe()}}:super._complete}unsubscribe(){var e;if(!this.shouldUnsubscribe||this.shouldUnsubscribe()){let{closed:t}=this;super.unsubscribe(),!t&&((e=this.onFinalize)===null||e===void 0||e.call(this))}}};var iv=hc(n=>function(){n(this),this.name="ObjectUnsubscribedError",this.message="object unsubscribed"});var Oi=(()=>{class n extends Us{constructor(){super(),this.closed=!1,this.currentObservers=null,this.observers=[],this.isStopped=!1,this.hasError=!1,this.thrownError=null}lift(t){let i=new vc(this,this);return i.operator=t,i}_throwIfClosed(){if(this.closed)throw new iv}next(t){Fs(()=>{if(this._throwIfClosed(),!this.isStopped){this.currentObservers||(this.currentObservers=Array.from(this.observers));for(let i of this.currentObservers)i.next(t)}})}error(t){Fs(()=>{if(this._throwIfClosed(),!this.isStopped){this.hasError=this.isStopped=!0,this.thrownError=t;let{observers:i}=this;for(;i.length;)i.shift().error(t)}})}complete(){Fs(()=>{if(this._throwIfClosed(),!this.isStopped){this.isStopped=!0;let{observers:t}=this;for(;t.length;)t.shift().complete()}})}unsubscribe(){this.isStopped=this.closed=!0,this.observers=this.currentObservers=null}get observed(){var t;return((t=this.observers)===null||t===void 0?void 0:t.length)>0}_trySubscribe(t){return this._throwIfClosed(),super._trySubscribe(t)}_subscribe(t){return this._throwIfClosed(),this._checkFinalizedStatuses(t),this._innerSubscribe(t)}_innerSubscribe(t){let{hasError:i,isStopped:r,observers:s}=this;return i||r?mf:(this.currentObservers=null,s.push(t),new mn(()=>{this.currentObservers=null,Uo(s,t)}))}_checkFinalizedStatuses(t){let{hasError:i,thrownError:r,isStopped:s}=this;i?t.error(r):s&&t.complete()}asObservable(){let t=new Us;return t.source=this,t}}return n.create=(e,t)=>new vc(e,t),n})(),vc=class extends Oi{constructor(e,t){super(),this.destination=e,this.source=t}next(e){var t,i;(i=(t=this.destination)===null||t===void 0?void 0:t.next)===null||i===void 0||i.call(t,e)}error(e){var t,i;(i=(t=this.destination)===null||t===void 0?void 0:t.error)===null||i===void 0||i.call(t,e)}complete(){var e,t;(t=(e=this.destination)===null||e===void 0?void 0:e.complete)===null||t===void 0||t.call(e)}_subscribe(e){var t,i;return(i=(t=this.source)===null||t===void 0?void 0:t.subscribe(e))!==null&&i!==void 0?i:mf}};var Bo=class extends Oi{constructor(e){super(),this._value=e}get value(){return this.getValue()}_subscribe(e){let t=super._subscribe(e);return!t.closed&&e.next(this._value),t}getValue(){let{hasError:e,thrownError:t,_value:i}=this;if(e)throw t;return this._throwIfClosed(),i}next(e){super.next(this._value=e)}};function bf(n,e){return tv((t,i)=>{let r=0;t.subscribe(nv(i,s=>{i.next(n.call(e,s,r++))}))})}var Sf;function yc(){return Sf}function mi(n){let e=Sf;return Sf=n,e}var rv=Symbol("NotFound");function Bs(n){return n===rv||n?.name==="\u0275NotFound"}var wc="https://angular.dev/best-practices/security#preventing-cross-site-scripting-xss",$e=class extends Error{code;constructor(e,t){super(Cc(e,t)),this.code=e}};function GM(n){return`NG0${Math.abs(n)}`}function Cc(n,e){return`${GM(n)}${e?": "+e:""}`}var dr=globalThis;function gt(n){for(let e in n)if(n[e]===gt)return e;throw Error("")}function Ic(n){if(typeof n=="string")return n;if(Array.isArray(n))return`[${n.map(Ic).join(", ")}]`;if(n==null)return""+n;let e=n.overriddenName||n.name;if(e)return`${e}`;let t=n.toString();if(t==null)return""+t;let i=t.indexOf(`
`);return i>=0?t.slice(0,i):t}function kf(n,e){return n?e?`${n} ${e}`:n:e||""}var WM=gt({__forward_ref__:gt});function Ac(n){return n.__forward_ref__=Ac,n}function En(n){return lv(n)?n():n}function lv(n){return typeof n=="function"&&n.hasOwnProperty(WM)&&n.__forward_ref__===Ac}function at(n){return{token:n.token,providedIn:n.providedIn||null,factory:n.factory,value:void 0}}function Dc(n){return jM(n,Rc)}function jM(n,e){return n.hasOwnProperty(e)&&n[e]||null}function $M(n){let e=n?.[Rc]??null;return e||null}function Tf(n){return n&&n.hasOwnProperty(xc)?n[xc]:null}var Rc=gt({\u0275prov:gt}),xc=gt({\u0275inj:gt}),it=class{_desc;ngMetadataName="InjectionToken";\u0275prov;constructor(e,t){this._desc=e,this.\u0275prov=void 0,typeof t=="number"?this.__NG_ELEMENT_ID__=t:t!==void 0&&(this.\u0275prov=at({token:this,providedIn:t.providedIn||"root",factory:t.factory}))}get multi(){return this}toString(){return`InjectionToken ${this._desc}`}};function Uf(n){return n&&!!n.\u0275providers}var Bf=gt({\u0275cmp:gt}),Vf=gt({\u0275dir:gt}),Hf=gt({\u0275pipe:gt});var wf=gt({\u0275fac:gt}),Xr=gt({__NG_ELEMENT_ID__:gt}),sv=gt({__NG_ENV_ID__:gt});function Yr(n){return Gf(n,"@Component"),n[Bf]||null}function zf(n){return Gf(n,"@Directive"),n[Vf]||null}function uv(n){return Gf(n,"@Pipe"),n[Hf]||null}function Gf(n,e){if(n==null)throw new $e(-919,!1)}function Zr(n){return typeof n=="string"?n:n==null?"":String(n)}var dv=gt({ngErrorCode:gt}),qM=gt({ngErrorMessage:gt}),XM=gt({ngTokenPath:gt});function Wf(n,e){return fv("",-200,e)}function Nc(n,e){throw new $e(-201,!1)}function fv(n,e,t){let i=new $e(e,n);return i[dv]=e,i[qM]=n,t&&(i[XM]=t),i}function YM(n){return n[dv]}var Cf;function hv(){return Cf}function gn(n){let e=Cf;return Cf=n,e}function jf(n,e,t){let i=Dc(n);if(i&&i.providedIn=="root")return i.value===void 0?i.value=i.factory():i.value;if(t&8)return null;if(e!==void 0)return e;Nc(n,"")}var ZM={},Gr=ZM,JM="__NG_DI_FLAG__",If=class{injector;constructor(e){this.injector=e}retrieve(e,t){let i=Wr(t)||0;try{return this.injector.get(e,i&8?null:Gr,i)}catch(r){if(Bs(r))return r;throw r}}};function KM(n,e=0){let t=yc();if(t===void 0)throw new $e(-203,!1);if(t===null)return jf(n,void 0,e);{let i=QM(e),r=t.retrieve(n,i);if(Bs(r)){if(i.optional)return null;throw r}return r}}function rt(n,e=0){return(hv()||KM)(En(n),e)}function me(n,e){return rt(n,Wr(e))}function Wr(n){return typeof n>"u"||typeof n=="number"?n:0|(n.optional&&8)|(n.host&&1)|(n.self&&2)|(n.skipSelf&&4)}function QM(n){return{optional:!!(n&8),host:!!(n&1),self:!!(n&2),skipSelf:!!(n&4)}}function Af(n){let e=[];for(let t=0;t<n.length;t++){let i=En(n[t]);if(Array.isArray(i)){if(i.length===0)throw new $e(900,!1);let r,s=0;for(let o=0;o<i.length;o++){let a=i[o],c=eb(a);typeof c=="number"?c===-1?r=a.token:s|=c:r=a}e.push(rt(r,s))}else e.push(rt(i))}return e}function eb(n){return n[JM]}function jr(n,e){let t=n.hasOwnProperty(wf);return t?n[wf]:null}function pv(n,e,t){if(n.length!==e.length)return!1;for(let i=0;i<n.length;i++){let r=n[i],s=e[i];if(t&&(r=t(r),s=t(s)),s!==r)return!1}return!0}function mv(n){return n.flat(Number.POSITIVE_INFINITY)}function Pc(n,e){n.forEach(t=>Array.isArray(t)?Pc(t,e):e(t))}function $f(n,e,t){e>=n.length?n.push(t):n.splice(e,0,t)}function jo(n,e){return e>=n.length-1?n.pop():n.splice(e,1)[0]}function gv(n,e,t,i){let r=n.length;if(r==e)n.push(t,i);else if(r===1)n.push(i,n[0]),n[0]=t;else{for(r--,n.push(n[r-1],n[r]);r>e;){let s=r-2;n[r]=n[s],r--}n[e]=t,n[e+1]=i}}function vv(n,e,t){let i=Hs(n,e);return i>=0?n[i|1]=t:(i=~i,gv(n,i,e,t)),i}function Lc(n,e){let t=Hs(n,e);if(t>=0)return n[t|1]}function Hs(n,e){return tb(n,e,1)}function tb(n,e,t){let i=0,r=n.length>>t;for(;r!==i;){let s=i+(r-i>>1),o=n[s<<t];if(e===o)return s<<t;o>e?r=s:i=s+1}return~(r<<t)}var Jr={},cr=[],Kr=new it(""),qf=new it("",-1),Xf=new it(""),Ho=class{get(e,t=Gr){if(t===Gr){let r=fv("",-201);throw r.name="\u0275NotFound",r}return t}};function $o(n){return{\u0275providers:n}}function yv(n){return $o([{provide:Kr,multi:!0,useValue:n}])}function _v(...n){return{\u0275providers:Yf(!0,n),\u0275fromNgModule:!0}}function Yf(n,...e){let t=[],i=new Set,r,s=o=>{t.push(o)};return Pc(e,o=>{let a=o;Mc(a,s,[],i)&&(r||=[],r.push(a))}),r!==void 0&&xv(r,s),t}function xv(n,e){for(let t=0;t<n.length;t++){let{ngModule:i,providers:r}=n[t];Zf(r,s=>{e(s,i)})}}function Mc(n,e,t,i){if(n=En(n),!n)return!1;let r=null,s=Tf(n),o=!s&&Yr(n);if(!s&&!o){let c=n.ngModule;if(s=Tf(c),s)r=c;else return!1}else{if(o&&!o.standalone)return!1;r=n}let a=i.has(r);if(o){if(a)return!1;if(i.add(r),o.dependencies){let c=typeof o.dependencies=="function"?o.dependencies():o.dependencies;for(let l of c)Mc(l,e,t,i)}}else if(s){if(s.imports!=null&&!a){i.add(r);let l;Pc(s.imports,u=>{Mc(u,e,t,i)&&(l||=[],l.push(u))}),l!==void 0&&xv(l,e)}if(!a){let l=jr(r)||(()=>new r);e({provide:r,useFactory:l,deps:cr},r),e({provide:Xf,useValue:r,multi:!0},r),e({provide:Kr,useValue:()=>rt(r),multi:!0},r)}let c=s.providers;if(c!=null&&!a){let l=n;Zf(c,u=>{e(u,l)})}}else return!1;return r!==n&&n.providers!==void 0}function Zf(n,e){for(let t of n)Uf(t)&&(t=t.\u0275providers),Array.isArray(t)?Zf(t,e):e(t)}var nb=gt({provide:String,useValue:gt});function Mv(n){return n!==null&&typeof n=="object"&&nb in n}function ib(n){return!!(n&&n.useExisting)}function rb(n){return!!(n&&n.useFactory)}function bc(n){return typeof n=="function"}var qo=new it(""),_c={},ov={},Ef;function Xo(){return Ef===void 0&&(Ef=new Ho),Ef}var Fn=class{},$r=class extends Fn{parent;source;scopes;records=new Map;_ngOnDestroyHooks=new Set;_onDestroyHooks=[];get destroyed(){return this._destroyed}_destroyed=!1;injectorDefTypes;constructor(e,t,i,r){super(),this.parent=t,this.source=i,this.scopes=r,Rf(e,o=>this.processProvider(o)),this.records.set(qf,Vs(void 0,this)),r.has("environment")&&this.records.set(Fn,Vs(void 0,this));let s=this.records.get(qo);s!=null&&typeof s.value=="string"&&this.scopes.add(s.value),this.injectorDefTypes=new Set(this.get(Xf,cr,{self:!0}))}retrieve(e,t){let i=Wr(t)||0;try{return this.get(e,Gr,i)}catch(r){if(Bs(r))return r;throw r}}destroy(){Vo(this),this._destroyed=!0;let e=Ae(null);try{for(let i of this._ngOnDestroyHooks)i.ngOnDestroy();let t=this._onDestroyHooks;this._onDestroyHooks=[];for(let i of t)i()}finally{this.records.clear(),this._ngOnDestroyHooks.clear(),this.injectorDefTypes.clear(),Ae(e)}}onDestroy(e){return Vo(this),this._onDestroyHooks.push(e),()=>this.removeOnDestroy(e)}runInContext(e){Vo(this);let t=mi(this),i=gn(void 0),r;try{return e()}finally{mi(t),gn(i)}}get(e,t=Gr,i){if(Vo(this),e.hasOwnProperty(sv))return e[sv](this);let r=Wr(i),s,o=mi(this),a=gn(void 0);try{if(!(r&4)){let l=this.records.get(e);if(l===void 0){let u=lb(e)&&Dc(e);u&&this.injectableDefInScope(u)?l=Vs(Df(e),_c):l=null,this.records.set(e,l)}if(l!=null)return this.hydrate(e,l,r)}let c=r&2?Xo():this.parent;return t=r&8&&t===Gr?null:t,c.get(e,t)}catch(c){let l=YM(c);throw l===-200||l===-201?new $e(l,null):c}finally{gn(a),mi(o)}}resolveInjectorInitializers(){let e=Ae(null),t=mi(this),i=gn(void 0),r;try{let s=this.get(Kr,cr,{self:!0});for(let o of s)o()}finally{mi(t),gn(i),Ae(e)}}toString(){return"R3Injector[...]"}processProvider(e){e=En(e);let t=bc(e)?e:En(e&&e.provide),i=ob(e);if(!bc(e)&&e.multi===!0){let r=this.records.get(t);r||(r=Vs(void 0,_c,!0),r.factory=()=>Af(r.multi),this.records.set(t,r)),t=e,r.multi.push(e)}this.records.set(t,i)}hydrate(e,t,i){let r=Ae(null);try{if(t.value===ov)throw Wf("");return t.value===_c&&(t.value=ov,t.value=t.factory(void 0,i)),typeof t.value=="object"&&t.value&&cb(t.value)&&this._ngOnDestroyHooks.add(t.value),t.value}finally{Ae(r)}}injectableDefInScope(e){if(!e.providedIn)return!1;let t=En(e.providedIn);return typeof t=="string"?t==="any"||this.scopes.has(t):this.injectorDefTypes.has(t)}removeOnDestroy(e){let t=this._onDestroyHooks.indexOf(e);t!==-1&&this._onDestroyHooks.splice(t,1)}};function Df(n){let e=Dc(n),t=e!==null?e.factory:jr(n);if(t!==null)return t;if(n instanceof it)throw new $e(-204,!1);if(n instanceof Function)return sb(n);throw new $e(-204,!1)}function sb(n){if(n.length>0)throw new $e(-204,!1);let t=$M(n);return t!==null?()=>t.factory(n):()=>new n}function ob(n){if(Mv(n))return Vs(void 0,n.useValue);{let e=bv(n);return Vs(e,_c)}}function bv(n,e,t){let i;if(bc(n)){let r=En(n);return jr(r)||Df(r)}else if(Mv(n))i=()=>En(n.useValue);else if(rb(n))i=()=>n.useFactory(...Af(n.deps||[]));else if(ib(n))i=(r,s)=>rt(En(n.useExisting),s!==void 0&&s&8?8:void 0);else{let r=En(n&&(n.useClass||n.provide));if(ab(n))i=()=>new r(...Af(n.deps));else return jr(r)||Df(r)}return i}function Vo(n){if(n.destroyed)throw new $e(-205,!1)}function Vs(n,e,t=!1){return{factory:n,value:e,multi:t?[]:void 0}}function ab(n){return!!n.deps}function cb(n){return n!==null&&typeof n=="object"&&typeof n.ngOnDestroy=="function"}function lb(n){return typeof n=="function"||typeof n=="object"&&n.ngMetadataName==="InjectionToken"}function Rf(n,e){for(let t of n)Array.isArray(t)?Rf(t,e):t&&Uf(t)?Rf(t.\u0275providers,e):e(t)}function Oc(n,e){let t;n instanceof $r?(Vo(n),t=n):t=new If(n);let i,r=mi(t),s=gn(void 0);try{return e()}finally{mi(r),gn(s)}}function Sv(){return hv()!==void 0||yc()!=null}var Kn=0,Ce=1,Le=2,Wt=3,Un=4,Bn=5,zs=6,Gs=7,Ft=8,Bi=9,Qn=10,wt=11,Ws=12,Jf=13,Qr=14,Vn=15,fr=16,es=17,vi=18,Vi=19,Kf=20,ki=21,Fc=22,lr=23,Tn=24,kc=25,hr=26,jt=27,Ev=1,Qf=6,pr=7,Yo=8,ts=9,At=10;function Hi(n){return Array.isArray(n)&&typeof n[Ev]=="object"}function ei(n){return Array.isArray(n)&&n[Ev]===!0}function eh(n){return(n.flags&4)!==0}function mr(n){return n.componentOffset>-1}function Uc(n){return(n.flags&1)===1}function ns(n){return!!n.template}function js(n){return(n[Le]&512)!==0}function is(n){return(n[Le]&256)===256}var Tv="svg",wv="math";function Hn(n){for(;Array.isArray(n);)n=n[Kn];return n}function th(n,e){return Hn(e[n])}function ti(n,e){return Hn(e[n.index])}function Bc(n,e){return n.data[e]}function Cv(n,e){return n[e]}function nh(n,e,t,i){t>=n.data.length&&(n.data[t]=null,n.blueprint[t]=null),e[t]=i}function ni(n,e){let t=e[n];return Hi(t)?t:t[Kn]}function Iv(n){return(n[Le]&4)===4}function Vc(n){return(n[Le]&128)===128}function Av(n){return ei(n[Wt])}function yi(n,e){return e==null?null:n[e]}function ih(n){n[es]=0}function rh(n){n[Le]&1024||(n[Le]|=1024,Vc(n)&&$s(n))}function Dv(n,e){for(;n>0;)e=e[Qr],n--;return e}function Zo(n){return!!(n[Le]&9216||n[Tn]?.dirty)}function Hc(n){n[Qn].changeDetectionScheduler?.notify(8),n[Le]&64&&(n[Le]|=1024),Zo(n)&&$s(n)}function $s(n){n[Qn].changeDetectionScheduler?.notify(0);let e=ur(n);for(;e!==null&&!(e[Le]&8192||(e[Le]|=8192,!Vc(e)));)e=ur(e)}function sh(n,e){if(is(n))throw new $e(911,!1);n[ki]===null&&(n[ki]=[]),n[ki].push(e)}function Rv(n,e){if(n[ki]===null)return;let t=n[ki].indexOf(e);t!==-1&&n[ki].splice(t,1)}function ur(n){let e=n[Wt];return ei(e)?e[Wt]:e}function oh(n){return n[Gs]??=[]}function ah(n){return n.cleanup??=[]}function Nv(n,e,t,i){let r=oh(e);r.push(t),n.firstCreatePass&&ah(n).push(i,r.length-1)}var qe={lFrame:qv(null),bindingsEnabled:!0,skipHydrationRootTNode:null};var Nf=!1;function Pv(){return qe.lFrame.elementDepthCount}function Lv(){qe.lFrame.elementDepthCount++}function ch(){qe.lFrame.elementDepthCount--}function Ov(){return qe.bindingsEnabled}function Fv(){return qe.skipHydrationRootTNode!==null}function lh(n){return qe.skipHydrationRootTNode===n}function uh(){qe.skipHydrationRootTNode=null}function Ye(){return qe.lFrame.lView}function sn(){return qe.lFrame.tView}function gr(n){return qe.lFrame.contextLView=n,n[Ft]}function vr(n){return qe.lFrame.contextLView=null,n}function wn(){let n=dh();for(;n!==null&&n.type===64;)n=n.parent;return n}function dh(){return qe.lFrame.currentTNode}function kv(){let n=qe.lFrame,e=n.currentTNode;return n.isParent?e:e.parent}function qs(n,e){let t=qe.lFrame;t.currentTNode=n,t.isParent=e}function fh(){return qe.lFrame.isParent}function Uv(){qe.lFrame.isParent=!1}function hh(){return Nf}function zo(n){let e=Nf;return Nf=n,e}function Bv(){let n=qe.lFrame,e=n.bindingRootIndex;return e===-1&&(e=n.bindingRootIndex=n.tView.bindingStartIndex),e}function Vv(){return qe.lFrame.bindingIndex}function Hv(n){return qe.lFrame.bindingIndex=n}function rs(){return qe.lFrame.bindingIndex++}function ph(n){let e=qe.lFrame,t=e.bindingIndex;return e.bindingIndex=e.bindingIndex+n,t}function zv(){return qe.lFrame.inI18n}function Gv(n,e){let t=qe.lFrame;t.bindingIndex=t.bindingRootIndex=n,zc(e)}function Wv(){return qe.lFrame.currentDirectiveIndex}function zc(n){qe.lFrame.currentDirectiveIndex=n}function jv(n){let e=qe.lFrame.currentDirectiveIndex;return e===-1?null:n[e]}function mh(){return qe.lFrame.currentQueryIndex}function Gc(n){qe.lFrame.currentQueryIndex=n}function ub(n){let e=n[Ce];return e.type===2?e.declTNode:e.type===1?n[Bn]:null}function gh(n,e,t){if(t&4){let r=e,s=n;for(;r=r.parent,r===null&&!(t&1);)if(r=ub(s),r===null||(s=s[Qr],r.type&10))break;if(r===null)return!1;e=r,n=s}let i=qe.lFrame=$v();return i.currentTNode=e,i.lView=n,!0}function Wc(n){let e=$v(),t=n[Ce];qe.lFrame=e,e.currentTNode=t.firstChild,e.lView=n,e.tView=t,e.contextLView=n,e.bindingIndex=t.bindingStartIndex,e.inI18n=!1}function $v(){let n=qe.lFrame,e=n===null?null:n.child;return e===null?qv(n):e}function qv(n){let e={currentTNode:null,isParent:!0,lView:null,tView:null,selectedIndex:-1,contextLView:null,elementDepthCount:0,currentNamespace:null,currentDirectiveIndex:-1,bindingRootIndex:-1,bindingIndex:-1,currentQueryIndex:0,parent:n,child:null,inI18n:!1};return n!==null&&(n.child=e),e}function Xv(){let n=qe.lFrame;return qe.lFrame=n.parent,n.currentTNode=null,n.lView=null,n}var vh=Xv;function jc(){let n=Xv();n.isParent=!0,n.tView=null,n.selectedIndex=-1,n.contextLView=null,n.elementDepthCount=0,n.currentDirectiveIndex=-1,n.currentNamespace=null,n.bindingRootIndex=-1,n.bindingIndex=-1,n.currentQueryIndex=0}function Yv(n){return(qe.lFrame.contextLView=Dv(n,qe.lFrame.contextLView))[Ft]}function zi(){return qe.lFrame.selectedIndex}function yr(n){qe.lFrame.selectedIndex=n}function $c(){let n=qe.lFrame;return Bc(n.tView,n.selectedIndex)}function Zv(){return qe.lFrame.currentNamespace}var Jv=!0;function qc(){return Jv}function Xc(n){Jv=n}function Pf(n,e=null,t=null,i){let r=Kv(n,e,t,i);return r.resolveInjectorInitializers(),r}function Kv(n,e=null,t=null,i,r=new Set){let s=[t||cr,_v(n)],o;return new $r(s,e||Xo(),o||null,r)}var gi=class n{static THROW_IF_NOT_FOUND=Gr;static NULL=new Ho;static create(e,t){if(Array.isArray(e))return Pf({name:""},t,e,"");{let i=e.name??"";return Pf({name:i},e.parent,e.providers,i)}}static \u0275prov=at({token:n,providedIn:"any",factory:()=>rt(qf)});static __NG_ELEMENT_ID__=-1},on=new it(""),Xs=(()=>{class n{static __NG_ELEMENT_ID__=db;static __NG_ENV_ID__=t=>t}return n})(),Sc=class extends Xs{_lView;constructor(e){super(),this._lView=e}get destroyed(){return is(this._lView)}onDestroy(e){let t=this._lView;return sh(t,e),()=>Rv(t,e)}};function db(){return new Sc(Ye())}var Qv=!1,ey=new it(""),Ys=(()=>{class n{taskId=0;pendingTasks=new Set;destroyed=!1;pendingTask=new Bo(!1);debugTaskTracker=me(ey,{optional:!0});get hasPendingTasks(){return this.destroyed?!1:this.pendingTask.value}get hasPendingTasksObservable(){return this.destroyed?new Us(t=>{t.next(!1),t.complete()}):this.pendingTask}add(){!this.hasPendingTasks&&!this.destroyed&&this.pendingTask.next(!0);let t=this.taskId++;return this.pendingTasks.add(t),this.debugTaskTracker?.add(t),t}has(t){return this.pendingTasks.has(t)}remove(t){this.pendingTasks.delete(t),this.debugTaskTracker?.remove(t),this.pendingTasks.size===0&&this.hasPendingTasks&&this.pendingTask.next(!1)}ngOnDestroy(){this.pendingTasks.clear(),this.hasPendingTasks&&this.pendingTask.next(!1),this.destroyed=!0,this.pendingTask.unsubscribe()}static \u0275prov=at({token:n,providedIn:"root",factory:()=>new n})}return n})(),Lf=class extends Oi{__isAsync;destroyRef=void 0;pendingTasks=void 0;constructor(e=!1){super(),this.__isAsync=e,Sv()&&(this.destroyRef=me(Xs,{optional:!0})??void 0,this.pendingTasks=me(Ys,{optional:!0})??void 0)}emit(e){let t=Ae(null);try{super.next(e)}finally{Ae(t)}}subscribe(e,t,i){let r=e,s=t||(()=>null),o=i;if(e&&typeof e=="object"){let c=e;r=c.next?.bind(c),s=c.error?.bind(c),o=c.complete?.bind(c)}this.__isAsync&&(s=this.wrapInTimeout(s),r&&(r=this.wrapInTimeout(r)),o&&(o=this.wrapInTimeout(o)));let a=super.subscribe({next:r,error:s,complete:o});return e instanceof mn&&e.add(a),a}wrapInTimeout(e){return t=>{let i=this.pendingTasks?.add();setTimeout(()=>{try{e(t)}finally{i!==void 0&&this.pendingTasks?.remove(i)}})}}},Fi=Lf;function Ec(...n){}function yh(n){let e,t;function i(){n=Ec;try{t!==void 0&&typeof cancelAnimationFrame=="function"&&cancelAnimationFrame(t),e!==void 0&&clearTimeout(e)}catch{}}return e=setTimeout(()=>{n(),i()}),typeof requestAnimationFrame=="function"&&(t=requestAnimationFrame(()=>{n(),i()})),()=>i()}function ty(n){return queueMicrotask(()=>n()),()=>{n=Ec}}var _h="isAngularZone",Go=_h+"_ID",fb=0,kn=class n{hasPendingMacrotasks=!1;hasPendingMicrotasks=!1;isStable=!0;onUnstable=new Fi(!1);onMicrotaskEmpty=new Fi(!1);onStable=new Fi(!1);onError=new Fi(!1);constructor(e){let{enableLongStackTrace:t=!1,shouldCoalesceEventChangeDetection:i=!1,shouldCoalesceRunChangeDetection:r=!1,scheduleInRootZone:s=Qv}=e;if(typeof Zone>"u")throw new $e(908,!1);Zone.assertZonePatched();let o=this;o._nesting=0,o._outer=o._inner=Zone.current,Zone.TaskTrackingZoneSpec&&(o._inner=o._inner.fork(new Zone.TaskTrackingZoneSpec)),t&&Zone.longStackTraceZoneSpec&&(o._inner=o._inner.fork(Zone.longStackTraceZoneSpec)),o.shouldCoalesceEventChangeDetection=!r&&i,o.shouldCoalesceRunChangeDetection=r,o.callbackScheduled=!1,o.scheduleInRootZone=s,mb(o)}static isInAngularZone(){return typeof Zone<"u"&&Zone.current.get(_h)===!0}static assertInAngularZone(){if(!n.isInAngularZone())throw new $e(909,!1)}static assertNotInAngularZone(){if(n.isInAngularZone())throw new $e(909,!1)}run(e,t,i){return this._inner.run(e,t,i)}runTask(e,t,i,r){let s=this._inner,o=s.scheduleEventTask("NgZoneEvent: "+r,e,hb,Ec,Ec);try{return s.runTask(o,t,i)}finally{s.cancelTask(o)}}runGuarded(e,t,i){return this._inner.runGuarded(e,t,i)}runOutsideAngular(e){return this._outer.run(e)}},hb={};function xh(n){if(n._nesting==0&&!n.hasPendingMicrotasks&&!n.isStable)try{n._nesting++,n.onMicrotaskEmpty.emit(null)}finally{if(n._nesting--,!n.hasPendingMicrotasks)try{n.runOutsideAngular(()=>n.onStable.emit(null))}finally{n.isStable=!0}}}function pb(n){if(n.isCheckStableRunning||n.callbackScheduled)return;n.callbackScheduled=!0;function e(){yh(()=>{n.callbackScheduled=!1,Of(n),n.isCheckStableRunning=!0,xh(n),n.isCheckStableRunning=!1})}n.scheduleInRootZone?Zone.root.run(()=>{e()}):n._outer.run(()=>{e()}),Of(n)}function mb(n){let e=()=>{pb(n)},t=fb++;n._inner=n._inner.fork({name:"angular",properties:{[_h]:!0,[Go]:t,[Go+t]:!0},onInvokeTask:(i,r,s,o,a,c)=>{if(gb(c))return i.invokeTask(s,o,a,c);try{return av(n),i.invokeTask(s,o,a,c)}finally{(n.shouldCoalesceEventChangeDetection&&o.type==="eventTask"||n.shouldCoalesceRunChangeDetection)&&e(),cv(n)}},onInvoke:(i,r,s,o,a,c,l)=>{try{return av(n),i.invoke(s,o,a,c,l)}finally{n.shouldCoalesceRunChangeDetection&&!n.callbackScheduled&&!vb(c)&&e(),cv(n)}},onHasTask:(i,r,s,o)=>{i.hasTask(s,o),r===s&&(o.change=="microTask"?(n._hasPendingMicrotasks=o.microTask,Of(n),xh(n)):o.change=="macroTask"&&(n.hasPendingMacrotasks=o.macroTask))},onHandleError:(i,r,s,o)=>(i.handleError(s,o),n.runOutsideAngular(()=>n.onError.emit(o)),!1)})}function Of(n){n._hasPendingMicrotasks||(n.shouldCoalesceEventChangeDetection||n.shouldCoalesceRunChangeDetection)&&n.callbackScheduled===!0?n.hasPendingMicrotasks=!0:n.hasPendingMicrotasks=!1}function av(n){n._nesting++,n.isStable&&(n.isStable=!1,n.onUnstable.emit(null))}function cv(n){n._nesting--,xh(n)}var Wo=class{hasPendingMicrotasks=!1;hasPendingMacrotasks=!1;isStable=!0;onUnstable=new Fi;onMicrotaskEmpty=new Fi;onStable=new Fi;onError=new Fi;run(e,t,i){return e.apply(t,i)}runGuarded(e,t,i){return e.apply(t,i)}runOutsideAngular(e){return e()}runTask(e,t,i,r){return e.apply(t,i)}};function gb(n){return ny(n,"__ignore_ng_zone__")}function vb(n){return ny(n,"__scheduler_tick__")}function ny(n,e){return!Array.isArray(n)||n.length!==1?!1:n[0]?.data?.[e]===!0}var Ui=class{_console=console;handleError(e){this._console.error("ERROR",e)}},ss=new it("",{factory:()=>{let n=me(kn),e=me(Fn),t;return i=>{n.runOutsideAngular(()=>{e.destroyed&&!t?setTimeout(()=>{throw i}):(t??=e.get(Ui),t.handleError(i))})}}}),iy={provide:Kr,useValue:()=>{let n=me(Ui,{optional:!0})},multi:!0},yb=new it("",{factory:()=>{let n=me(on).defaultView;if(!n)return;let e=me(ss),t=s=>{e(s.reason),s.preventDefault()},i=s=>{s.error?e(s.error):e(new Error(s.message,{cause:s})),s.preventDefault()},r=()=>{n.addEventListener("unhandledrejection",t),n.addEventListener("error",i)};typeof Zone<"u"?Zone.root.run(r):r(),me(Xs).onDestroy(()=>{n.removeEventListener("error",i),n.removeEventListener("unhandledrejection",t)})}});function Mh(){return $o([yv(()=>{me(yb)})])}function an(n,e){let[t,i,r]=ff(n,e?.equal),s=t,o=s[yn];return s.set=i,s.update=r,s.asReadonly=ry.bind(s),s}function ry(){let n=this[yn];if(n.readonlyFn===void 0){let e=()=>this();e[yn]=n,n.readonlyFn=e}return n.readonlyFn}var Yc=(()=>{class n{view;node;constructor(t,i){this.view=t,this.node=i}static __NG_ELEMENT_ID__=_b}return n})();function _b(){return new Yc(Ye(),wn())}var qr=class{},Jo=new it("",{factory:()=>!0});var bh=new it("");var Zc=(()=>{class n{static \u0275prov=at({token:n,providedIn:"root",factory:()=>new Ff})}return n})(),Ff=class{dirtyEffectCount=0;queues=new Map;add(e){this.enqueue(e),this.schedule(e)}schedule(e){e.dirty&&this.dirtyEffectCount++}remove(e){let t=e.zone,i=this.queues.get(t);i.has(e)&&(i.delete(e),e.dirty&&this.dirtyEffectCount--)}enqueue(e){let t=e.zone;this.queues.has(t)||this.queues.set(t,new Set);let i=this.queues.get(t);i.has(e)||i.add(e)}flush(){for(;this.dirtyEffectCount>0;){let e=!1;for(let[t,i]of this.queues)t===null?e||=this.flushQueue(i):e||=t.run(()=>this.flushQueue(i));e||(this.dirtyEffectCount=0)}}flushQueue(e){let t=!1;for(let i of e)i.dirty&&(this.dirtyEffectCount--,t=!0,i.run());return t}},Tc=class{[yn];constructor(e){this[yn]=e}destroy(){this[yn].destroy()}};function Sh(n,e){let t=e?.injector??me(gi),i=e?.manualCleanup!==!0?t.get(Xs):null,r,s=t.get(Yc,null,{optional:!0}),o=t.get(qr);return s!==null?(r=bb(s.view,o,n),i instanceof Sc&&i._lView===s.view&&(i=null)):r=Sb(n,t.get(Zc),o),r.injector=t,i!==null&&(r.onDestroyFns=[i.onDestroy(()=>r.destroy())]),new Tc(r)}var sy=Gt(It({},hf),{cleanupFns:void 0,zone:null,onDestroyFns:null,run(){let n=zo(!1);try{pf(this)}finally{zo(n)}},cleanup(){if(!this.cleanupFns?.length)return;let n=Ae(null);try{for(;this.cleanupFns.length;)this.cleanupFns.pop()()}finally{this.cleanupFns=[],Ae(n)}}}),xb=Gt(It({},sy),{consumerMarkedDirty(){this.scheduler.schedule(this),this.notifier.notify(12)},destroy(){if(Vr(this),this.onDestroyFns!==null)for(let n of this.onDestroyFns)n();this.cleanup(),this.scheduler.remove(this)}}),Mb=Gt(It({},sy),{consumerMarkedDirty(){this.view[Le]|=8192,$s(this.view),this.notifier.notify(13)},destroy(){if(Vr(this),this.onDestroyFns!==null)for(let n of this.onDestroyFns)n();this.cleanup(),this.view[lr]?.delete(this)}});function bb(n,e,t){let i=Object.create(Mb);return i.view=n,i.zone=typeof Zone<"u"?Zone.current:null,i.notifier=e,i.fn=oy(i,t),n[lr]??=new Set,n[lr].add(i),i.consumerMarkedDirty(i),i}function Sb(n,e,t){let i=Object.create(xb);return i.fn=oy(i,n),i.scheduler=e,i.notifier=t,i.zone=typeof Zone<"u"?Zone.current:null,i.scheduler.add(i),i.notifier.notify(12),i}function oy(n,e){return()=>{e(t=>(n.cleanupFns??=[]).push(t))}}function pp(n){return{toString:n}.toString()}function Lb(n){return typeof n=="function"}function Uy(n,e,t,i){e!==null?e.applyValueToInputSignal(e,i):n[t]=i}var ol=class{previousValue;currentValue;firstChange;constructor(e,t,i){this.previousValue=e,this.currentValue=t,this.firstChange=i}isFirstChange(){return this.firstChange}};function Ob(n){return n.type.prototype.ngOnChanges&&(n.setInput=kb),Fb}function Fb(){let n=Vy(this),e=n?.current;if(e){let t=n.previous;if(t===Jr)n.previous=e;else for(let i in e)t[i]=e[i];n.current=null,this.ngOnChanges(e)}}function kb(n,e,t,i,r){let s=this.declaredInputs[i],o=Vy(n)||Ub(n,{previous:Jr,current:null}),a=o.current||(o.current={}),c=o.previous,l=c[s];a[s]=new ol(l&&l.currentValue,t,c===Jr),Uy(n,e,r,t)}var By="__ngSimpleChanges__";function Vy(n){return n[By]||null}function Ub(n,e){return n[By]=e}var ay=[];var vt=function(n,e=null,t){for(let i=0;i<ay.length;i++){let r=ay[i];r(n,e,t)}},ct=(function(n){return n[n.TemplateCreateStart=0]="TemplateCreateStart",n[n.TemplateCreateEnd=1]="TemplateCreateEnd",n[n.TemplateUpdateStart=2]="TemplateUpdateStart",n[n.TemplateUpdateEnd=3]="TemplateUpdateEnd",n[n.LifecycleHookStart=4]="LifecycleHookStart",n[n.LifecycleHookEnd=5]="LifecycleHookEnd",n[n.OutputStart=6]="OutputStart",n[n.OutputEnd=7]="OutputEnd",n[n.BootstrapApplicationStart=8]="BootstrapApplicationStart",n[n.BootstrapApplicationEnd=9]="BootstrapApplicationEnd",n[n.BootstrapComponentStart=10]="BootstrapComponentStart",n[n.BootstrapComponentEnd=11]="BootstrapComponentEnd",n[n.ChangeDetectionStart=12]="ChangeDetectionStart",n[n.ChangeDetectionEnd=13]="ChangeDetectionEnd",n[n.ChangeDetectionSyncStart=14]="ChangeDetectionSyncStart",n[n.ChangeDetectionSyncEnd=15]="ChangeDetectionSyncEnd",n[n.AfterRenderHooksStart=16]="AfterRenderHooksStart",n[n.AfterRenderHooksEnd=17]="AfterRenderHooksEnd",n[n.ComponentStart=18]="ComponentStart",n[n.ComponentEnd=19]="ComponentEnd",n[n.DeferBlockStateStart=20]="DeferBlockStateStart",n[n.DeferBlockStateEnd=21]="DeferBlockStateEnd",n[n.DynamicComponentStart=22]="DynamicComponentStart",n[n.DynamicComponentEnd=23]="DynamicComponentEnd",n[n.HostBindingsUpdateStart=24]="HostBindingsUpdateStart",n[n.HostBindingsUpdateEnd=25]="HostBindingsUpdateEnd",n})(ct||{});function Bb(n,e,t){let{ngOnChanges:i,ngOnInit:r,ngDoCheck:s}=e.type.prototype;if(i){let o=Ob(e);(t.preOrderHooks??=[]).push(n,o),(t.preOrderCheckHooks??=[]).push(n,o)}r&&(t.preOrderHooks??=[]).push(0-n,r),s&&((t.preOrderHooks??=[]).push(n,s),(t.preOrderCheckHooks??=[]).push(n,s))}function Vb(n,e){for(let t=e.directiveStart,i=e.directiveEnd;t<i;t++){let s=n.data[t].type.prototype,{ngAfterContentInit:o,ngAfterContentChecked:a,ngAfterViewInit:c,ngAfterViewChecked:l,ngOnDestroy:u}=s;o&&(n.contentHooks??=[]).push(-t,o),a&&((n.contentHooks??=[]).push(t,a),(n.contentCheckHooks??=[]).push(t,a)),c&&(n.viewHooks??=[]).push(-t,c),l&&((n.viewHooks??=[]).push(t,l),(n.viewCheckHooks??=[]).push(t,l)),u!=null&&(n.destroyHooks??=[]).push(t,u)}}function tl(n,e,t){Hy(n,e,3,t)}function nl(n,e,t,i){(n[Le]&3)===t&&Hy(n,e,t,i)}function Eh(n,e){let t=n[Le];(t&3)===e&&(t&=16383,t+=1,n[Le]=t)}function Hy(n,e,t,i){let r=i!==void 0?n[es]&65535:0,s=i??-1,o=e.length-1,a=0;for(let c=r;c<o;c++)if(typeof e[c+1]=="number"){if(a=e[c],i!=null&&a>=i)break}else e[c]<0&&(n[es]+=65536),(a<s||s==-1)&&(Hb(n,t,e,c),n[es]=(n[es]&4294901760)+c+2),c++}function cy(n,e){vt(ct.LifecycleHookStart,n,e);let t=Ae(null);try{e.call(n)}finally{Ae(t),vt(ct.LifecycleHookEnd,n,e)}}function Hb(n,e,t,i){let r=t[i]<0,s=t[i+1],o=r?-t[i]:t[i],a=n[o];r?n[Le]>>14<n[es]>>16&&(n[Le]&3)===e&&(n[Le]+=16384,cy(a,s)):cy(a,s)}var Js=-1,ta=class{factory;name;injectImpl;resolving=!1;canSeeViewProviders;multi;componentProviders;index;providerFactory;constructor(e,t,i,r){this.factory=e,this.name=r,this.canSeeViewProviders=t,this.injectImpl=i}};function zb(n){return(n.flags&8)!==0}function Gb(n){return(n.flags&16)!==0}function Wb(n,e,t){let i=0;for(;i<t.length;){let r=t[i];if(typeof r=="number"){if(r!==0)break;i++;let s=t[i++],o=t[i++],a=t[i++];n.setAttribute(e,o,a,s)}else{let s=r,o=t[++i];$b(s)?n.setProperty(e,s,o):n.setAttribute(e,s,o),i++}}return i}function jb(n){return n===3||n===4||n===6}function $b(n){return n.charCodeAt(0)===64}function xl(n,e){if(!(e===null||e.length===0))if(n===null||n.length===0)n=e.slice();else{let t=-1;for(let i=0;i<e.length;i++){let r=e[i];typeof r=="number"?t=r:t===0||(t===-1||t===2?ly(n,t,r,null,e[++i]):ly(n,t,r,null,null))}}return n}function ly(n,e,t,i,r){let s=0,o=n.length;if(e===-1)o=-1;else for(;s<n.length;){let a=n[s++];if(typeof a=="number"){if(a===e){o=-1;break}else if(a>e){o=s-1;break}}}for(;s<n.length;){let a=n[s];if(typeof a=="number")break;if(a===t){r!==null&&(n[s+1]=r);return}s++,r!==null&&s++}o!==-1&&(n.splice(o,0,e),s=o+1),n.splice(s++,0,t),r!==null&&n.splice(s++,0,r)}function zy(n){return n!==Js}function al(n){return n&32767}function qb(n){return n>>16}function cl(n,e){let t=qb(n),i=e;for(;t>0;)i=i[Qr],t--;return i}var Rh=!0;function ll(n){let e=Rh;return Rh=n,e}var Xb=256,Gy=Xb-1,Wy=5,Yb=0,_i={};function Zb(n,e,t){let i;typeof t=="string"?i=t.charCodeAt(0)||0:t.hasOwnProperty(Xr)&&(i=t[Xr]),i==null&&(i=t[Xr]=Yb++);let r=i&Gy,s=1<<r;e.data[n+(r>>Wy)]|=s}function jy(n,e){let t=$y(n,e);if(t!==-1)return t;let i=e[Ce];i.firstCreatePass&&(n.injectorIndex=e.length,Th(i.data,n),Th(e,null),Th(i.blueprint,null));let r=mp(n,e),s=n.injectorIndex;if(zy(r)){let o=al(r),a=cl(r,e),c=a[Ce].data;for(let l=0;l<8;l++)e[s+l]=a[o+l]|c[o+l]}return e[s+8]=r,s}function Th(n,e){n.push(0,0,0,0,0,0,0,0,e)}function $y(n,e){return n.injectorIndex===-1||n.parent&&n.parent.injectorIndex===n.injectorIndex||e[n.injectorIndex+8]===null?-1:n.injectorIndex}function mp(n,e){if(n.parent&&n.parent.injectorIndex!==-1)return n.parent.injectorIndex;let t=0,i=null,r=e;for(;r!==null;){if(i=Jy(r),i===null)return Js;if(t++,r=r[Qr],i.injectorIndex!==-1)return i.injectorIndex|t<<16}return Js}function Jb(n,e,t){Zb(n,e,t)}function qy(n,e,t){if(t&8||n!==void 0)return n;Nc(e,"NodeInjector")}function Xy(n,e,t,i){if(t&8&&i===void 0&&(i=null),(t&3)===0){let r=n[Bi],s=gn(void 0);try{return r?r.get(e,i,t&8):jf(e,i,t&8)}finally{gn(s)}}return qy(i,e,t)}function Yy(n,e,t,i=0,r){if(n!==null){if(e[Le]&2048&&!(i&2)){let o=tS(n,e,t,i,_i);if(o!==_i)return o}let s=Zy(n,e,t,i,_i);if(s!==_i)return s}return Xy(e,t,i,r)}function Zy(n,e,t,i,r){let s=Qb(t);if(typeof s=="function"){if(!gh(e,n,i))return i&1?qy(r,t,i):Xy(e,t,i,r);try{let o;if(o=s(i),o==null&&!(i&8))Nc(t);else return o}finally{vh()}}else if(typeof s=="number"){let o=null,a=$y(n,e),c=Js,l=i&1?e[Vn][Bn]:null;for((a===-1||i&4)&&(c=a===-1?mp(n,e):e[a+8],c===Js||!dy(i,!1)?a=-1:(o=e[Ce],a=al(c),e=cl(c,e)));a!==-1;){let u=e[Ce];if(uy(s,a,u.data)){let f=Kb(a,e,t,o,i,l);if(f!==_i)return f}c=e[a+8],c!==Js&&dy(i,e[Ce].data[a+8]===l)&&uy(s,a,e)?(o=u,a=al(c),e=cl(c,e)):a=-1}}return r}function Kb(n,e,t,i,r,s){let o=e[Ce],a=o.data[n+8],c=i==null?mr(a)&&Rh:i!=o&&(a.type&3)!==0,l=r&1&&s===a,u=il(a,o,t,c,l);return u!==null?ul(e,o,u,a,r):_i}function il(n,e,t,i,r){let s=n.providerIndexes,o=e.data,a=s&1048575,c=n.directiveStart,l=n.directiveEnd,u=s>>20,f=i?a:a+u,d=r?a+u:l;for(let h=f;h<d;h++){let g=o[h];if(h<c&&t===g||h>=c&&g.type===t)return h}if(r){let h=o[c];if(h&&ns(h)&&h.type===t)return c}return null}function ul(n,e,t,i,r){let s=n[t],o=e.data;if(s instanceof ta){let a=s;if(a.resolving)throw Wf("");let c=ll(a.canSeeViewProviders);a.resolving=!0;let l=o[t].type||o[t],u,f=a.injectImpl?gn(a.injectImpl):null,d=gh(n,i,0);try{s=n[t]=a.factory(void 0,r,o,n,i),e.firstCreatePass&&t>=i.directiveStart&&Bb(t,o[t],e)}finally{f!==null&&gn(f),ll(c),a.resolving=!1,vh()}}return s}function Qb(n){if(typeof n=="string")return n.charCodeAt(0)||0;let e=n.hasOwnProperty(Xr)?n[Xr]:void 0;return typeof e=="number"?e>=0?e&Gy:eS:e}function uy(n,e,t){let i=1<<n;return!!(t[e+(n>>Wy)]&i)}function dy(n,e){return!(n&2)&&!(n&1&&e)}var os=class{_tNode;_lView;constructor(e,t){this._tNode=e,this._lView=t}get(e,t,i){return Yy(this._tNode,this._lView,e,Wr(i),t)}};function eS(){return new os(wn(),Ye())}function tS(n,e,t,i,r){let s=n,o=e;for(;s!==null&&o!==null&&o[Le]&2048&&!js(o);){let a=Zy(s,o,t,i|2,_i);if(a!==_i)return a;let c=s.parent;if(!c){let l=o[Kf];if(l){let u=l.get(t,_i,i&-5);if(u!==_i)return u}c=Jy(o),o=o[Qr]}s=c}return r}function Jy(n){let e=n[Ce],t=e.type;return t===2?e.declTNode:t===1?n[Bn]:null}function nS(){return to(wn(),Ye())}function to(n,e){return new us(ti(n,e))}var us=(()=>{class n{nativeElement;constructor(t){this.nativeElement=t}static __NG_ELEMENT_ID__=nS}return n})();function iS(n){return n instanceof us?n.nativeElement:n}function rS(){return this._results[Symbol.iterator]()}var dl=class{_emitDistinctChangesOnly;dirty=!0;_onDirty=void 0;_results=[];_changesDetected=!1;_changes=void 0;length=0;first=void 0;last=void 0;get changes(){return this._changes??=new Oi}constructor(e=!1){this._emitDistinctChangesOnly=e}get(e){return this._results[e]}map(e){return this._results.map(e)}filter(e){return this._results.filter(e)}find(e){return this._results.find(e)}reduce(e,t){return this._results.reduce(e,t)}forEach(e){this._results.forEach(e)}some(e){return this._results.some(e)}toArray(){return this._results.slice()}toString(){return this._results.toString()}reset(e,t){this.dirty=!1;let i=mv(e);(this._changesDetected=!pv(this._results,i,t))&&(this._results=i,this.length=i.length,this.last=i[this.length-1],this.first=i[0])}notifyOnChanges(){this._changes!==void 0&&(this._changesDetected||!this._emitDistinctChangesOnly)&&this._changes.next(this)}onDirty(e){this._onDirty=e}setDirty(){this.dirty=!0,this._onDirty?.()}destroy(){this._changes!==void 0&&(this._changes.complete(),this._changes.unsubscribe())}[Symbol.iterator]=rS};function Ky(n){return(n.flags&128)===128}var gp=(function(n){return n[n.OnPush=0]="OnPush",n[n.Eager=1]="Eager",n[n.Default=1]="Default",n})(gp||{}),Qy=new Map,sS=0;function oS(){return sS++}function aS(n){Qy.set(n[Vi],n)}function Nh(n){Qy.delete(n[Vi])}var fy="__ngContext__";function Ks(n,e){Hi(e)?(n[fy]=e[Vi],aS(e)):n[fy]=e}function e_(n){return n_(n[Ws])}function t_(n){return n_(n[Un])}function n_(n){for(;n!==null&&!ei(n);)n=n[Un];return n}var Ph;function vp(n){Ph=n}function i_(){if(Ph!==void 0)return Ph;if(typeof document<"u")return document;throw new $e(210,!1)}var Ml=new it("",{factory:()=>cS}),cS="ng";var bl=new it(""),la=new it("",{providedIn:"platform",factory:()=>"unknown"});var Sl=new it("",{factory:()=>me(on).body?.querySelector("[ngCspNonce]")?.getAttribute("ngCspNonce")||null});var r_="r";var s_="di";var o_=!1,a_=new it("",{factory:()=>o_});var lS=(n,e,t,i)=>{};function uS(n,e,t,i){lS(n,e,t,i)}function yp(n){return(n.flags&32)===32}var dS=()=>null;function c_(n,e,t=!1){return dS(n,e,t)}function l_(n,e){let t=n.contentQueries;if(t!==null){let i=Ae(null);try{for(let r=0;r<t.length;r+=2){let s=t[r],o=t[r+1];if(o!==-1){let a=n.data[o];Gc(s),a.contentQueries(2,e[o],o)}}}finally{Ae(i)}}}function Lh(n,e,t){Gc(0);let i=Ae(null);try{e(n,t)}finally{Ae(i)}}function u_(n,e,t){if(eh(e)){let i=Ae(null);try{let r=e.directiveStart,s=e.directiveEnd;for(let o=r;o<s;o++){let a=n.data[o];if(a.contentQueries){let c=t[o];a.contentQueries(1,c,o)}}}finally{Ae(i)}}}var ri=(function(n){return n[n.Emulated=0]="Emulated",n[n.None=2]="None",n[n.ShadowDom=3]="ShadowDom",n[n.ExperimentalIsolatedShadowDom=4]="ExperimentalIsolatedShadowDom",n})(ri||{});var Jc;function fS(){if(Jc===void 0&&(Jc=null,dr.trustedTypes))try{Jc=dr.trustedTypes.createPolicy("angular",{createHTML:n=>n,createScript:n=>n,createScriptURL:n=>n})}catch{}return Jc}function El(n){return fS()?.createHTML(n)||n}var Kc;function hS(){if(Kc===void 0&&(Kc=null,dr.trustedTypes))try{Kc=dr.trustedTypes.createPolicy("angular#unsafe-bypass",{createHTML:n=>n,createScript:n=>n,createScriptURL:n=>n})}catch{}return Kc}function hy(n){return hS()?.createHTML(n)||n}var Gi=class{changingThisBreaksApplicationSecurity;constructor(e){this.changingThisBreaksApplicationSecurity=e}toString(){return`SafeValue must use [property]=binding: ${this.changingThisBreaksApplicationSecurity} (see ${wc})`}},Oh=class extends Gi{getTypeName(){return"HTML"}},Fh=class extends Gi{getTypeName(){return"Style"}},kh=class extends Gi{getTypeName(){return"Script"}},Uh=class extends Gi{getTypeName(){return"URL"}},Bh=class extends Gi{getTypeName(){return"ResourceURL"}};function Mi(n){return n instanceof Gi?n.changingThisBreaksApplicationSecurity:n}function ji(n,e){let t=d_(n);if(t!=null&&t!==e){if(t==="ResourceURL"&&e==="URL")return!0;throw new Error(`Required a safe ${e}, got a ${t} (see ${wc})`)}return t===e}function d_(n){return n instanceof Gi&&n.getTypeName()||null}function _p(n){return new Oh(n)}function xp(n){return new Fh(n)}function Mp(n){return new kh(n)}function bp(n){return new Uh(n)}function Sp(n){return new Bh(n)}function pS(n){let e=new Hh(n);return mS()?new Vh(e):e}var Vh=class{inertDocumentHelper;constructor(e){this.inertDocumentHelper=e}getInertBodyElement(e){e="<body><remove></remove>"+e;try{let t=new window.DOMParser().parseFromString(El(e),"text/html").body;return t===null?this.inertDocumentHelper.getInertBodyElement(e):(t.firstChild?.remove(),t)}catch{return null}}},Hh=class{defaultDoc;inertDocument;constructor(e){this.defaultDoc=e,this.inertDocument=this.defaultDoc.implementation.createHTMLDocument("sanitization-inert")}getInertBodyElement(e){let t=this.inertDocument.createElement("template");return t.innerHTML=El(e),t}};function mS(){try{return!!new window.DOMParser().parseFromString(El(""),"text/html")}catch{return!1}}var gS=/^(?!javascript:)(?:[a-z0-9+.-]+:|[^&:\/?#]*(?:[\/?#]|$))/i;function ua(n){return n=String(n),n.match(gS)?n:"unsafe:"+n}function $i(n){let e={};for(let t of n.split(","))e[t]=!0;return e}function da(...n){let e={};for(let t of n)for(let i in t)t.hasOwnProperty(i)&&(e[i]=!0);return e}var f_=$i("area,br,col,hr,img,wbr"),h_=$i("colgroup,dd,dt,li,p,tbody,td,tfoot,th,thead,tr"),p_=$i("rp,rt"),vS=da(p_,h_),yS=da(h_,$i("address,article,aside,blockquote,caption,center,del,details,dialog,dir,div,dl,figure,figcaption,footer,h1,h2,h3,h4,h5,h6,header,hgroup,hr,ins,main,map,menu,nav,ol,pre,section,summary,table,ul")),_S=da(p_,$i("a,abbr,acronym,audio,b,bdi,bdo,big,br,cite,code,del,dfn,em,font,i,img,ins,kbd,label,map,mark,picture,q,ruby,rp,rt,s,samp,small,source,span,strike,strong,sub,sup,time,track,tt,u,var,video")),py=da(f_,yS,_S,vS),m_=$i("background,cite,href,itemtype,longdesc,poster,src,xlink:href"),xS=$i("abbr,accesskey,align,alt,autoplay,axis,bgcolor,border,cellpadding,cellspacing,class,clear,color,cols,colspan,compact,controls,coords,datetime,default,dir,download,face,headers,height,hidden,hreflang,hspace,ismap,itemscope,itemprop,kind,label,lang,language,loop,media,muted,nohref,nowrap,open,preload,rel,rev,role,rows,rowspan,rules,scope,scrolling,shape,size,sizes,span,srclang,srcset,start,summary,tabindex,target,title,translate,type,usemap,valign,value,vspace,width"),MS=$i("aria-activedescendant,aria-atomic,aria-autocomplete,aria-busy,aria-checked,aria-colcount,aria-colindex,aria-colspan,aria-controls,aria-current,aria-describedby,aria-details,aria-disabled,aria-dropeffect,aria-errormessage,aria-expanded,aria-flowto,aria-grabbed,aria-haspopup,aria-hidden,aria-invalid,aria-keyshortcuts,aria-label,aria-labelledby,aria-level,aria-live,aria-modal,aria-multiline,aria-multiselectable,aria-orientation,aria-owns,aria-placeholder,aria-posinset,aria-pressed,aria-readonly,aria-relevant,aria-required,aria-roledescription,aria-rowcount,aria-rowindex,aria-rowspan,aria-selected,aria-setsize,aria-sort,aria-valuemax,aria-valuemin,aria-valuenow,aria-valuetext"),bS=da(m_,xS,MS),SS=$i("script,style,template");var zh=class{sanitizedSomething=!1;buf=[];sanitizeChildren(e){let t=e.firstChild,i=!0,r=[];for(;t;){if(t.nodeType===Node.ELEMENT_NODE?i=this.startElement(t):t.nodeType===Node.TEXT_NODE?this.chars(t.nodeValue):this.sanitizedSomething=!0,i&&t.firstChild){r.push(t),t=wS(t);continue}for(;t;){t.nodeType===Node.ELEMENT_NODE&&this.endElement(t);let s=TS(t);if(s){t=s;break}t=r.pop()}}return this.buf.join("")}startElement(e){let t=my(e).toLowerCase();if(!py.hasOwnProperty(t))return this.sanitizedSomething=!0,!SS.hasOwnProperty(t);this.buf.push("<"),this.buf.push(t);let i=e.attributes;for(let r=0;r<i.length;r++){let s=i.item(r),o=s.name,a=o.toLowerCase();if(!bS.hasOwnProperty(a)){this.sanitizedSomething=!0;continue}let c=s.value;m_[a]&&(c=ua(c)),this.buf.push(" ",o,'="',gy(c),'"')}return this.buf.push(">"),!0}endElement(e){let t=my(e).toLowerCase();py.hasOwnProperty(t)&&!f_.hasOwnProperty(t)&&(this.buf.push("</"),this.buf.push(t),this.buf.push(">"))}chars(e){this.buf.push(gy(e))}};function ES(n,e){return(n.compareDocumentPosition(e)&Node.DOCUMENT_POSITION_CONTAINED_BY)!==Node.DOCUMENT_POSITION_CONTAINED_BY}function TS(n){let e=n.nextSibling;if(e&&n!==e.previousSibling)throw g_(e);return e}function wS(n){let e=n.firstChild;if(e&&ES(n,e))throw g_(e);return e}function my(n){let e=n.nodeName;return typeof e=="string"?e:"FORM"}function g_(n){return new Error(`Failed to sanitize html because the element is clobbered: ${n.outerHTML}`)}var CS=/[\uD800-\uDBFF][\uDC00-\uDFFF]/g,IS=/([^\#-~ |!])/g;function gy(n){return n.replace(/&/g,"&amp;").replace(CS,function(e){let t=e.charCodeAt(0),i=e.charCodeAt(1);return"&#"+((t-55296)*1024+(i-56320)+65536)+";"}).replace(IS,function(e){return"&#"+e.charCodeAt(0)+";"}).replace(/</g,"&lt;").replace(/>/g,"&gt;")}var Qc;function Tl(n,e){let t=null;try{Qc=Qc||pS(n);let i=e?String(e):"";t=Qc.getInertBodyElement(i);let r=5,s=i;do{if(r===0)throw new Error("Failed to sanitize html because the input is unstable");r--,i=s,s=t.innerHTML,t=Qc.getInertBodyElement(i)}while(i!==s);let a=new zh().sanitizeChildren(vy(t)||t);return El(a)}finally{if(t){let i=vy(t)||t;for(;i.firstChild;)i.firstChild.remove()}}}function vy(n){return"content"in n&&AS(n)?n.content:null}function AS(n){return n.nodeType===Node.ELEMENT_NODE&&n.nodeName==="TEMPLATE"}function DS(n,e){return n.createText(e)}function RS(n,e,t){n.setValue(e,t)}function v_(n,e,t){return n.createElement(e,t)}function fl(n,e,t,i,r){n.insertBefore(e,t,i,r)}function y_(n,e,t){n.appendChild(e,t)}function yy(n,e,t,i,r){i!==null?fl(n,e,t,i,r):y_(n,e,t)}function __(n,e,t,i){n.removeChild(null,e,t,i)}function NS(n,e,t){n.setAttribute(e,"style",t)}function PS(n,e,t){t===""?n.removeAttribute(e,"class"):n.setAttribute(e,"class",t)}function x_(n,e,t){let{mergedAttrs:i,classes:r,styles:s}=t;i!==null&&Wb(n,e,i),r!==null&&PS(n,e,r),s!==null&&NS(n,e,s)}var Cn=(function(n){return n[n.NONE=0]="NONE",n[n.HTML=1]="HTML",n[n.STYLE=2]="STYLE",n[n.SCRIPT=3]="SCRIPT",n[n.URL=4]="URL",n[n.RESOURCE_URL=5]="RESOURCE_URL",n})(Cn||{});function Je(n){let e=M_();return e?hy(e.sanitize(Cn.HTML,n)||""):ji(n,"HTML")?hy(Mi(n)):Tl(i_(),Zr(n))}function qi(n){let e=M_();return e?e.sanitize(Cn.URL,n)||"":ji(n,"URL")?Mi(n):ua(Zr(n))}function M_(){let n=Ye();return n&&n[Qn].sanitizer}function LS(n,e,t){let i=n.length;for(;;){let r=n.indexOf(e,t);if(r===-1)return r;if(r===0||n.charCodeAt(r-1)<=32){let s=e.length;if(r+s===i||n.charCodeAt(r+s)<=32)return r}t=r+1}}var b_="ng-template";function OS(n,e,t,i){let r=0;if(i){for(;r<e.length&&typeof e[r]=="string";r+=2)if(e[r]==="class"&&LS(e[r+1].toLowerCase(),t,0)!==-1)return!0}else if(Ep(n))return!1;if(r=e.indexOf(1,r),r>-1){let s;for(;++r<e.length&&typeof(s=e[r])=="string";)if(s.toLowerCase()===t)return!0}return!1}function Ep(n){return n.type===4&&n.value!==b_}function FS(n,e,t){let i=n.type===4&&!t?b_:n.value;return e===i}function kS(n,e,t){let i=4,r=n.attrs,s=r!==null?VS(r):0,o=!1;for(let a=0;a<e.length;a++){let c=e[a];if(typeof c=="number"){if(!o&&!ii(i)&&!ii(c))return!1;if(o&&ii(c))continue;o=!1,i=c|i&1;continue}if(!o)if(i&4){if(i=2|i&1,c!==""&&!FS(n,c,t)||c===""&&e.length===1){if(ii(i))return!1;o=!0}}else if(i&8){if(r===null||!OS(n,r,c,t)){if(ii(i))return!1;o=!0}}else{let l=e[++a],u=US(c,r,Ep(n),t);if(u===-1){if(ii(i))return!1;o=!0;continue}if(l!==""){let f;if(u>s?f="":f=r[u+1].toLowerCase(),i&2&&l!==f){if(ii(i))return!1;o=!0}}}}return ii(i)||o}function ii(n){return(n&1)===0}function US(n,e,t,i){if(e===null)return-1;let r=0;if(i||!t){let s=!1;for(;r<e.length;){let o=e[r];if(o===n)return r;if(o===3||o===6)s=!0;else if(o===1||o===2){let a=e[++r];for(;typeof a=="string";)a=e[++r];continue}else{if(o===4)break;if(o===0){r+=4;continue}}r+=s?1:2}return-1}else return HS(e,n)}function BS(n,e,t=!1){for(let i=0;i<e.length;i++)if(kS(n,e[i],t))return!0;return!1}function VS(n){for(let e=0;e<n.length;e++){let t=n[e];if(jb(t))return e}return n.length}function HS(n,e){let t=n.indexOf(4);if(t>-1)for(t++;t<n.length;){let i=n[t];if(typeof i=="number")return-1;if(i===e)return t;t++}return-1}function _y(n,e){return n?":not("+e.trim()+")":e}function zS(n){let e=n[0],t=1,i=2,r="",s=!1;for(;t<n.length;){let o=n[t];if(typeof o=="string")if(i&2){let a=n[++t];r+="["+o+(a.length>0?'="'+a+'"':"")+"]"}else i&8?r+="."+o:i&4&&(r+=" "+o);else r!==""&&!ii(o)&&(e+=_y(s,r),r=""),i=o,s=s||!ii(i);t++}return r!==""&&(e+=_y(s,r)),e}function GS(n){return n.map(zS).join(",")}function WS(n){let e=[],t=[],i=1,r=2;for(;i<n.length;){let s=n[i];if(typeof s=="string")r===2?s!==""&&e.push(s,n[++i]):r===8&&t.push(s);else{if(!ii(r))break;r=s}i++}return t.length&&e.push(1,...t),e}var zn={};function Tp(n,e,t,i,r,s,o,a,c,l,u){let f=jt+i,d=f+r,h=jS(f,d),g=typeof l=="function"?l():l;return h[Ce]={type:n,blueprint:h,template:t,queries:null,viewQuery:a,declTNode:e,data:h.slice().fill(null,f),bindingStartIndex:f,expandoStartIndex:d,hostBindingOpCodes:null,firstCreatePass:!0,firstUpdatePass:!0,staticViewQueries:!1,staticContentQueries:!1,preOrderHooks:null,preOrderCheckHooks:null,contentHooks:null,contentCheckHooks:null,viewHooks:null,viewCheckHooks:null,destroyHooks:null,cleanup:null,contentQueries:null,components:null,directiveRegistry:typeof s=="function"?s():s,pipeRegistry:typeof o=="function"?o():o,firstChild:null,schemas:c,consts:g,incompleteFirstPass:!1,ssrId:u}}function jS(n,e){let t=[];for(let i=0;i<e;i++)t.push(i<n?null:zn);return t}function $S(n){let e=n.tView;return e===null||e.incompleteFirstPass?n.tView=Tp(1,null,n.template,n.decls,n.vars,n.directiveDefs,n.pipeDefs,n.viewQuery,n.schemas,n.consts,n.id):e}function wp(n,e,t,i,r,s,o,a,c,l,u){let f=e.blueprint.slice();return f[Kn]=r,f[Le]=i|4|128|8|64|1024,(l!==null||n&&n[Le]&2048)&&(f[Le]|=2048),ih(f),f[Wt]=f[Qr]=n,f[Ft]=t,f[Qn]=o||n&&n[Qn],f[wt]=a||n&&n[wt],f[Bi]=c||n&&n[Bi]||null,f[Bn]=s,f[Vi]=oS(),f[zs]=u,f[Kf]=l,f[Vn]=e.type==2?n[Vn]:f,f}function qS(n,e,t){let i=ti(e,n),r=$S(t),s=n[Qn].rendererFactory,o=Cp(n,wp(n,r,null,S_(t),i,e,null,s.createRenderer(i,t),null,null,null));return n[e.index]=o}function S_(n){let e=16;return n.signals?e=4096:n.onPush&&(e=64),e}function E_(n,e,t,i){if(t===0)return-1;let r=e.length;for(let s=0;s<t;s++)e.push(i),n.blueprint.push(i),n.data.push(null);return r}function Cp(n,e){return n[Ws]?n[Jf][Un]=e:n[Ws]=e,n[Jf]=e,e}function le(n=1){T_(sn(),Ye(),zi()+n,!1)}function T_(n,e,t,i){if(!i)if((e[Le]&3)===3){let s=n.preOrderCheckHooks;s!==null&&tl(e,s,t)}else{let s=n.preOrderHooks;s!==null&&nl(e,s,0,t)}yr(t)}var wl=(function(n){return n[n.None=0]="None",n[n.SignalBased=1]="SignalBased",n[n.HasDecoratorInputTransform=2]="HasDecoratorInputTransform",n})(wl||{});function Gh(n,e,t,i){let r=Ae(null);try{let[s,o,a]=n.inputs[t],c=null;(o&wl.SignalBased)!==0&&(c=e[s][yn]),c!==null&&c.transformFn!==void 0?i=c.transformFn(i):a!==null&&(i=a.call(e,i)),n.setInput!==null?n.setInput(e,c,i,t,s):Uy(e,c,s,i)}finally{Ae(r)}}var Wi=(function(n){return n[n.Important=1]="Important",n[n.DashCase=2]="DashCase",n})(Wi||{}),XS;function Ip(n,e){return XS(n,e)}var jL=typeof document<"u"&&typeof document?.documentElement?.getAnimations=="function";var Wh=new WeakMap,Ko=new WeakSet;function YS(n,e){let t=Wh.get(n);if(!t||t.length===0)return;let i=e.parentNode,r=e.previousSibling;for(let s=t.length-1;s>=0;s--){let o=t[s],a=o.parentNode;o===e?(t.splice(s,1),Ko.add(o),o.dispatchEvent(new CustomEvent("animationend",{detail:{cancel:!0}}))):(r&&o===r||a&&i&&a!==i)&&(t.splice(s,1),o.dispatchEvent(new CustomEvent("animationend",{detail:{cancel:!0}})),o.parentNode?.removeChild(o))}}function ZS(n,e){let t=Wh.get(n);t?t.includes(e)||t.push(e):Wh.set(n,[e])}var as=new Set,Ap=(function(n){return n[n.CHANGE_DETECTION=0]="CHANGE_DETECTION",n[n.AFTER_NEXT_RENDER=1]="AFTER_NEXT_RENDER",n})(Ap||{}),no=new it(""),xy=new Set;function io(n){xy.has(n)||(xy.add(n),performance?.mark?.("mark_feature_usage",{detail:{feature:n}}))}var w_=(()=>{class n{impl=null;execute(){this.impl?.execute()}static \u0275prov=at({token:n,providedIn:"root",factory:()=>new n})}return n})();var C_=new it("",{factory:()=>({queue:new Set,isScheduled:!1,scheduler:null,injector:me(Fn)})});function I_(n,e,t){let i=n.get(C_);if(Array.isArray(e))for(let r of e)i.queue.add(r),t?.detachedLeaveAnimationFns?.push(r);else i.queue.add(e),t?.detachedLeaveAnimationFns?.push(e);i.scheduler&&i.scheduler(n)}function JS(n,e){let t=n.get(C_);if(e.detachedLeaveAnimationFns){for(let i of e.detachedLeaveAnimationFns)t.queue.delete(i);e.detachedLeaveAnimationFns=void 0}}function KS(n,e){for(let[t,i]of e)I_(n,i.animateFns)}function My(n,e,t,i){let r=n?.[hr]?.enter;e!==null&&r&&r.has(t.index)&&KS(i,r)}function Zs(n,e,t,i,r,s,o,a){if(r!=null){let c,l=!1;ei(r)?c=r:Hi(r)&&(l=!0,r=r[Kn]);let u=Hn(r);n===0&&i!==null?(My(a,i,s,t),o==null?y_(e,i,u):fl(e,i,u,o||null,!0)):n===1&&i!==null?(My(a,i,s,t),fl(e,i,u,o||null,!0),YS(s,u)):n===2?(a?.[hr]?.leave?.has(s.index)&&ZS(s,u),Ko.delete(u),by(a,s,t,f=>{if(Ko.has(u)){Ko.delete(u);return}__(e,u,l,f)})):n===3&&(Ko.delete(u),by(a,s,t,()=>{e.destroyNode(u)})),c!=null&&dE(e,n,t,c,s,i,o)}}function QS(n,e){A_(n,e),e[Kn]=null,e[Bn]=null}function eE(n,e,t,i,r,s){i[Kn]=r,i[Bn]=e,Il(n,i,t,1,r,s)}function A_(n,e){e[Qn].changeDetectionScheduler?.notify(9),Il(n,e,e[wt],2,null,null)}function tE(n){let e=n[Ws];if(!e)return wh(n[Ce],n);for(;e;){let t=null;if(Hi(e))t=e[Ws];else{let i=e[At];i&&(t=i)}if(!t){for(;e&&!e[Un]&&e!==n;)Hi(e)&&wh(e[Ce],e),e=e[Wt];e===null&&(e=n),Hi(e)&&wh(e[Ce],e),t=e&&e[Un]}e=t}}function Dp(n,e){let t=n[ts],i=t.indexOf(e);t.splice(i,1)}function Cl(n,e){if(is(e))return;let t=e[wt];t.destroyNode&&Il(n,e,t,3,null,null),tE(e)}function wh(n,e){if(is(e))return;let t=Ae(null);try{e[Le]&=-129,e[Le]|=256,e[Tn]&&Vr(e[Tn]),rE(n,e),iE(n,e),e[Ce].type===1&&e[wt].destroy();let i=e[fr];if(i!==null&&ei(e[Wt])){i!==e[Wt]&&Dp(i,e);let r=e[vi];r!==null&&r.detachView(n)}Nh(e)}finally{Ae(t)}}function by(n,e,t,i){let r=n?.[hr];if(r==null||r.leave==null||!r.leave.has(e.index))return i(!1);n&&as.add(n[Vi]),I_(t,()=>{if(r.leave&&r.leave.has(e.index)){let o=r.leave.get(e.index),a=[];if(o){for(let c=0;c<o.animateFns.length;c++){let l=o.animateFns[c],{promise:u}=l();a.push(u)}r.detachedLeaveAnimationFns=void 0}r.running=Promise.allSettled(a),nE(n,i)}else n&&as.delete(n[Vi]),i(!1)},r)}function nE(n,e){let t=n[hr]?.running;if(t){t.then(()=>{n[hr].running=void 0,as.delete(n[Vi]),e(!0)});return}e(!1)}function iE(n,e){let t=n.cleanup,i=e[Gs];if(t!==null)for(let o=0;o<t.length-1;o+=2)if(typeof t[o]=="string"){let a=t[o+3];a>=0?i[a]():i[-a].unsubscribe(),o+=2}else{let a=i[t[o+1]];t[o].call(a)}i!==null&&(e[Gs]=null);let r=e[ki];if(r!==null){e[ki]=null;for(let o=0;o<r.length;o++){let a=r[o];a()}}let s=e[lr];if(s!==null){e[lr]=null;for(let o of s)o.destroy()}}function rE(n,e){let t;if(n!=null&&(t=n.destroyHooks)!=null)for(let i=0;i<t.length;i+=2){let r=e[t[i]];if(!(r instanceof ta)){let s=t[i+1];if(Array.isArray(s))for(let o=0;o<s.length;o+=2){let a=r[s[o]],c=s[o+1];vt(ct.LifecycleHookStart,a,c);try{c.call(a)}finally{vt(ct.LifecycleHookEnd,a,c)}}else{vt(ct.LifecycleHookStart,r,s);try{s.call(r)}finally{vt(ct.LifecycleHookEnd,r,s)}}}}}function sE(n,e,t){return oE(n,e.parent,t)}function oE(n,e,t){let i=e;for(;i!==null&&i.type&168;)e=i,i=e.parent;if(i===null)return t[Kn];if(mr(i)){let{encapsulation:r}=n.data[i.directiveStart+i.componentOffset];if(r===ri.None||r===ri.Emulated)return null}return ti(i,t)}function aE(n,e,t){return lE(n,e,t)}function cE(n,e,t){return n.type&40?ti(n,t):null}var lE=cE,Sy;function Rp(n,e,t,i){let r=sE(n,i,e),s=e[wt],o=i.parent||e[Bn],a=aE(o,i,e);if(r!=null)if(Array.isArray(t))for(let c=0;c<t.length;c++)yy(s,r,t[c],a,!1);else yy(s,r,t,a,!1);Sy!==void 0&&Sy(s,i,e,t,r)}function Qo(n,e){if(e!==null){let t=e.type;if(t&3)return ti(e,n);if(t&4)return jh(-1,n[e.index]);if(t&8){let i=e.child;if(i!==null)return Qo(n,i);{let r=n[e.index];return ei(r)?jh(-1,r):Hn(r)}}else{if(t&128)return Qo(n,e.next);if(t&32)return Ip(e,n)()||Hn(n[e.index]);{let i=D_(n,e);if(i!==null){if(Array.isArray(i))return i[0];let r=ur(n[Vn]);return Qo(r,i)}else return Qo(n,e.next)}}}return null}function D_(n,e){if(e!==null){let i=n[Vn][Bn],r=e.projection;return i.projection[r]}return null}function jh(n,e){let t=At+n+1;if(t<e.length){let i=e[t],r=i[Ce].firstChild;if(r!==null)return Qo(i,r)}return e[pr]}function Np(n,e,t,i,r,s,o){for(;t!=null;){let a=i[Bi];if(t.type===128){t=t.next;continue}let c=i[t.index],l=t.type;if(o&&e===0&&(c&&Ks(Hn(c),i),t.flags|=2),!yp(t))if(l&8)Np(n,e,t.child,i,r,s,!1),Zs(e,n,a,r,c,t,s,i);else if(l&32){let u=Ip(t,i),f;for(;f=u();)Zs(e,n,a,r,f,t,s,i);Zs(e,n,a,r,c,t,s,i)}else l&16?uE(n,e,i,t,r,s):Zs(e,n,a,r,c,t,s,i);t=o?t.projectionNext:t.next}}function Il(n,e,t,i,r,s){Np(t,i,n.firstChild,e,r,s,!1)}function uE(n,e,t,i,r,s){let o=t[Vn],c=o[Bn].projection[i.projection];if(Array.isArray(c))for(let l=0;l<c.length;l++){let u=c[l];Zs(e,n,t[Bi],r,u,i,s,t)}else{let l=c,u=o[Wt];Ky(i)&&(l.flags|=128),Np(n,e,l,u,r,s,!0)}}function dE(n,e,t,i,r,s,o){let a=i[pr],c=Hn(i);a!==c&&Zs(e,n,t,s,a,r,o);for(let l=At;l<i.length;l++){let u=i[l];Il(u[Ce],u,n,e,s,a)}}function fE(n,e,t,i,r){if(e)r?n.addClass(t,i):n.removeClass(t,i);else{let s=i.indexOf("-")===-1?void 0:Wi.DashCase;r==null?n.removeStyle(t,i,s):(typeof r=="string"&&r.endsWith("!important")&&(r=r.slice(0,-10),s|=Wi.Important),n.setStyle(t,i,r,s))}}function R_(n,e,t,i,r){let s=zi(),o=i&2;try{yr(-1),o&&e.length>jt&&T_(n,e,jt,!1);let a=o?ct.TemplateUpdateStart:ct.TemplateCreateStart;vt(a,r,t),t(i,r)}finally{yr(s);let a=o?ct.TemplateUpdateEnd:ct.TemplateCreateEnd;vt(a,r,t)}}function N_(n,e,t){_E(n,e,t),(t.flags&64)===64&&xE(n,e,t)}function Pp(n,e,t=ti){let i=e.localNames;if(i!==null){let r=e.index+1;for(let s=0;s<i.length;s+=2){let o=i[s+1],a=o===-1?t(e,n):n[o];n[r++]=a}}}function hE(n,e,t,i){let s=i.get(a_,o_)||t===ri.ShadowDom||t===ri.ExperimentalIsolatedShadowDom,o=n.selectRootElement(e,s);return pE(o),o}function pE(n){mE(n)}var mE=()=>null;function gE(n){return n==="class"?"className":n==="for"?"htmlFor":n==="formaction"?"formAction":n==="innerHtml"?"innerHTML":n==="readonly"?"readOnly":n==="tabindex"?"tabIndex":n}function vE(n,e,t,i,r,s){let o=e[Ce];if(Lp(n,o,e,t,i)){mr(n)&&yE(e,n.index);return}n.type&3&&(t=gE(t)),P_(n,e,t,i,r,s)}function P_(n,e,t,i,r,s){if(n.type&3){let o=ti(n,e);i=s!=null?s(i,n.value||"",t):i,r.setProperty(o,t,i)}else n.type&12}function yE(n,e){let t=ni(e,n);t[Le]&16||(t[Le]|=64)}function _E(n,e,t){let i=t.directiveStart,r=t.directiveEnd;mr(t)&&qS(e,t,n.data[i+t.componentOffset]),n.firstCreatePass||jy(t,e);let s=t.initialInputs;for(let o=i;o<r;o++){let a=n.data[o],c=ul(e,n,o,t);if(Ks(c,e),s!==null&&TE(e,o-i,c,a,t,s),ns(a)){let l=ni(t.index,e);l[Ft]=ul(e,n,o,t)}}}function xE(n,e,t){let i=t.directiveStart,r=t.directiveEnd,s=t.index,o=Wv();try{yr(s);for(let a=i;a<r;a++){let c=n.data[a],l=e[a];zc(a),(c.hostBindings!==null||c.hostVars!==0||c.hostAttrs!==null)&&ME(c,l)}}finally{yr(-1),zc(o)}}function ME(n,e){n.hostBindings!==null&&n.hostBindings(1,e)}function bE(n,e){let t=n.directiveRegistry,i=null;if(t)for(let r=0;r<t.length;r++){let s=t[r];BS(e,s.selectors,!1)&&(i??=[],ns(s)?i.unshift(s):i.push(s))}return i}function SE(n,e,t,i,r,s){let o=ti(n,e);EE(e[wt],o,s,n.value,t,i,r)}function EE(n,e,t,i,r,s,o){if(s==null)n.removeAttribute(e,r,t);else{let a=o==null?Zr(s):o(s,i||"",r);n.setAttribute(e,r,a,t)}}function TE(n,e,t,i,r,s){let o=s[e];if(o!==null)for(let a=0;a<o.length;a+=2){let c=o[a],l=o[a+1];Gh(i,t,c,l)}}function L_(n,e,t,i,r){let s=jt+t,o=e[Ce],a=r(o,e,n,i,t);e[s]=a,qs(n,!0);let c=n.type===2;return c?(x_(e[wt],a,n),(Pv()===0||Uc(n))&&Ks(a,e),Lv()):Ks(a,e),qc()&&(!c||!yp(n))&&Rp(o,e,a,n),n}function O_(n){let e=n;return fh()?Uv():(e=e.parent,qs(e,!1)),e}function wE(n,e){let t=n[Bi];if(!t)return;let i;try{i=t.get(ss,null)}catch{i=null}i?.(e)}function Lp(n,e,t,i,r){let s=n.inputs?.[i],o=n.hostDirectiveInputs?.[i],a=!1;if(o)for(let c=0;c<o.length;c+=2){let l=o[c],u=o[c+1],f=e.data[l];Gh(f,t[l],u,r),a=!0}if(s)for(let c of s){let l=t[c],u=e.data[c];Gh(u,l,i,r),a=!0}return a}function CE(n,e){let t=ni(e,n),i=t[Ce];IE(i,t);let r=t[Kn];r!==null&&t[zs]===null&&(t[zs]=c_(r,t[Bi])),vt(ct.ComponentStart);try{Op(i,t,t[Ft])}finally{vt(ct.ComponentEnd,t[Ft])}}function IE(n,e){for(let t=e.length;t<n.blueprint.length;t++)e.push(n.blueprint[t])}function Op(n,e,t){Wc(e);try{let i=n.viewQuery;i!==null&&Lh(1,i,t);let r=n.template;r!==null&&R_(n,e,r,1,t),n.firstCreatePass&&(n.firstCreatePass=!1),e[vi]?.finishViewCreation(n),n.staticContentQueries&&l_(n,e),n.staticViewQueries&&Lh(2,n.viewQuery,t);let s=n.components;s!==null&&AE(e,s)}catch(i){throw n.firstCreatePass&&(n.incompleteFirstPass=!0,n.firstCreatePass=!1),i}finally{e[Le]&=-5,jc()}}function AE(n,e){for(let t=0;t<e.length;t++)CE(n,e[t])}function Al(n,e,t,i){let r=Ae(null);try{let s=e.tView,a=n[Le]&4096?4096:16,c=wp(n,s,t,a,null,e,null,null,i?.injector??null,i?.embeddedViewInjector??null,i?.dehydratedView??null),l=n[e.index];c[fr]=l;let u=n[vi];return u!==null&&(c[vi]=u.createEmbeddedView(s)),Op(s,c,t),c}finally{Ae(r)}}function na(n,e){return!e||e.firstChild===null||Ky(n)}function ia(n,e,t,i,r=!1){for(;t!==null;){if(t.type===128){t=r?t.projectionNext:t.next;continue}let s=e[t.index];s!==null&&i.push(Hn(s)),ei(s)&&F_(s,i);let o=t.type;if(o&8)ia(n,e,t.child,i);else if(o&32){let a=Ip(t,e),c;for(;c=a();)i.push(c)}else if(o&16){let a=D_(e,t);if(Array.isArray(a))i.push(...a);else{let c=ur(e[Vn]);ia(c[Ce],c,a,i,!0)}}t=r?t.projectionNext:t.next}return i}function F_(n,e){for(let t=At;t<n.length;t++){let i=n[t],r=i[Ce].firstChild;r!==null&&ia(i[Ce],i,r,e)}n[pr]!==n[Kn]&&e.push(n[pr])}function k_(n){if(n[kc]!==null){for(let e of n[kc])e.impl.addSequence(e);n[kc].length=0}}var U_=[];function DE(n){return n[Tn]??RE(n)}function RE(n){let e=U_.pop()??Object.create(PE);return e.lView=n,e}function NE(n){n.lView[Tn]!==n&&(n.lView=null,U_.push(n))}var PE=Gt(It({},Ns),{consumerIsAlwaysLive:!0,kind:"template",consumerMarkedDirty:n=>{$s(n.lView)},consumerOnSignalRead(){this.lView[Tn]=this}});function LE(n){let e=n[Tn]??Object.create(OE);return e.lView=n,e}var OE=Gt(It({},Ns),{consumerIsAlwaysLive:!0,kind:"template",consumerMarkedDirty:n=>{let e=ur(n.lView);for(;e&&!B_(e[Ce]);)e=ur(e);e&&rh(e)},consumerOnSignalRead(){this.lView[Tn]=this}});function B_(n){return n.type!==2}function V_(n){if(n[lr]===null)return;let e=!0;for(;e;){let t=!1;for(let i of n[lr])i.dirty&&(t=!0,i.zone===null||Zone.current===i.zone?i.run():i.zone.run(()=>i.run()));e=t&&!!(n[Le]&8192)}}var FE=100;function H_(n,e=0){let i=n[Qn].rendererFactory,r=!1;r||i.begin?.();try{kE(n,e)}finally{r||i.end?.()}}function kE(n,e){let t=hh();try{zo(!0),$h(n,e);let i=0;for(;Zo(n);){if(i===FE)throw new $e(103,!1);i++,$h(n,1)}}finally{zo(t)}}function UE(n,e,t,i){if(is(e))return;let r=e[Le],s=!1,o=!1;Wc(e);let a=!0,c=null,l=null;s||(B_(n)?(l=DE(e),c=Ps(l)):uc()===null?(a=!1,l=LE(e),c=Ps(l)):e[Tn]&&(Vr(e[Tn]),e[Tn]=null));try{ih(e),Hv(n.bindingStartIndex),t!==null&&R_(n,e,t,2,i);let u=(r&3)===3;if(!s)if(u){let h=n.preOrderCheckHooks;h!==null&&tl(e,h,null)}else{let h=n.preOrderHooks;h!==null&&nl(e,h,0,null),Eh(e,0)}if(o||BE(e),V_(e),z_(e,0),n.contentQueries!==null&&l_(n,e),!s)if(u){let h=n.contentCheckHooks;h!==null&&tl(e,h)}else{let h=n.contentHooks;h!==null&&nl(e,h,1),Eh(e,1)}HE(n,e);let f=n.components;f!==null&&W_(e,f,0);let d=n.viewQuery;if(d!==null&&Lh(2,d,i),!s)if(u){let h=n.viewCheckHooks;h!==null&&tl(e,h)}else{let h=n.viewHooks;h!==null&&nl(e,h,2),Eh(e,2)}if(n.firstUpdatePass===!0&&(n.firstUpdatePass=!1),e[Fc]){for(let h of e[Fc])h();e[Fc]=null}s||(k_(e),e[Le]&=-73)}catch(u){throw s||$s(e),u}finally{l!==null&&(Oo(l,c),a&&NE(l)),jc()}}function z_(n,e){for(let t=e_(n);t!==null;t=t_(t))for(let i=At;i<t.length;i++){let r=t[i];G_(r,e)}}function BE(n){for(let e=e_(n);e!==null;e=t_(e)){if(!(e[Le]&2))continue;let t=e[ts];for(let i=0;i<t.length;i++){let r=t[i];rh(r)}}}function VE(n,e,t){vt(ct.ComponentStart);let i=ni(e,n);try{G_(i,t)}finally{vt(ct.ComponentEnd,i[Ft])}}function G_(n,e){Vc(n)&&$h(n,e)}function $h(n,e){let i=n[Ce],r=n[Le],s=n[Tn],o=!!(e===0&&r&16);if(o||=!!(r&64&&e===0),o||=!!(r&1024),o||=!!(s?.dirty&&Fo(s)),o||=!1,s&&(s.dirty=!1),n[Le]&=-9217,o)UE(i,n,i.template,n[Ft]);else if(r&8192){let a=Ae(null);try{V_(n),z_(n,1);let c=i.components;c!==null&&W_(n,c,1),k_(n)}finally{Ae(a)}}}function W_(n,e,t){for(let i=0;i<e.length;i++)VE(n,e[i],t)}function HE(n,e){let t=n.hostBindingOpCodes;if(t!==null)try{for(let i=0;i<t.length;i++){let r=t[i];if(r<0)yr(~r);else{let s=r,o=t[++i],a=t[++i];Gv(o,s);let c=e[s];vt(ct.HostBindingsUpdateStart,c);try{a(2,c)}finally{vt(ct.HostBindingsUpdateEnd,c)}}}}finally{yr(-1)}}function Fp(n,e){let t=hh()?64:1088;for(n[Qn].changeDetectionScheduler?.notify(e);n;){n[Le]|=t;let i=ur(n);if(js(n)&&!i)return n;n=i}return null}function j_(n,e,t,i){return[n,!0,0,e,null,i,null,t,null,null]}function $_(n,e){let t=At+e;if(t<n.length)return n[t]}function Dl(n,e,t,i=!0){let r=e[Ce];if(zE(r,e,n,t),i){let o=jh(t,n),a=e[wt],c=a.parentNode(n[pr]);c!==null&&eE(r,n[Bn],a,e,c,o)}let s=e[zs];s!==null&&s.firstChild!==null&&(s.firstChild=null)}function q_(n,e){let t=ra(n,e);return t!==void 0&&Cl(t[Ce],t),t}function ra(n,e){if(n.length<=At)return;let t=At+e,i=n[t];if(i){let r=i[fr];r!==null&&r!==n&&Dp(r,i),e>0&&(n[t-1][Un]=i[Un]);let s=jo(n,At+e);QS(i[Ce],i);let o=s[vi];o!==null&&o.detachView(s[Ce]),i[Wt]=null,i[Un]=null,i[Le]&=-129}return i}function zE(n,e,t,i){let r=At+i,s=t.length;i>0&&(t[r-1][Un]=e),i<s-At?(e[Un]=t[r],$f(t,At+i,e)):(t.push(e),e[Un]=null),e[Wt]=t;let o=e[fr];o!==null&&t!==o&&X_(o,e);let a=e[vi];a!==null&&a.insertView(n),Hc(e),e[Le]|=128}function X_(n,e){let t=n[ts],i=e[Wt];if(Hi(i))n[Le]|=2;else{let r=i[Wt][Vn];e[Vn]!==r&&(n[Le]|=2)}t===null?n[ts]=[e]:t.push(e)}var Qs=class{_lView;_cdRefInjectingView;_appRef=null;_attachedToViewContainer=!1;exhaustive;get rootNodes(){let e=this._lView,t=e[Ce];return ia(t,e,t.firstChild,[])}constructor(e,t){this._lView=e,this._cdRefInjectingView=t}get context(){return this._lView[Ft]}set context(e){this._lView[Ft]=e}get destroyed(){return is(this._lView)}destroy(){if(this._appRef)this._appRef.detachView(this);else if(this._attachedToViewContainer){let e=this._lView[Wt];if(ei(e)){let t=e[Yo],i=t?t.indexOf(this):-1;i>-1&&(ra(e,i),jo(t,i))}this._attachedToViewContainer=!1}Cl(this._lView[Ce],this._lView)}onDestroy(e){sh(this._lView,e)}markForCheck(){Fp(this._cdRefInjectingView||this._lView,4)}detach(){this._lView[Le]&=-129}reattach(){Hc(this._lView),this._lView[Le]|=128}detectChanges(){this._lView[Le]|=1024,H_(this._lView)}checkNoChanges(){}attachToViewContainerRef(){if(this._appRef)throw new $e(902,!1);this._attachedToViewContainer=!0}detachFromAppRef(){this._appRef=null;let e=js(this._lView),t=this._lView[fr];t!==null&&!e&&Dp(t,this._lView),A_(this._lView[Ce],this._lView)}attachToAppRef(e){if(this._attachedToViewContainer)throw new $e(902,!1);this._appRef=e;let t=js(this._lView),i=this._lView[fr];i!==null&&!t&&X_(i,this._lView),Hc(this._lView)}};var sa=(()=>{class n{_declarationLView;_declarationTContainer;elementRef;static __NG_ELEMENT_ID__=GE;constructor(t,i,r){this._declarationLView=t,this._declarationTContainer=i,this.elementRef=r}get ssrId(){return this._declarationTContainer.tView?.ssrId||null}createEmbeddedView(t,i){return this.createEmbeddedViewImpl(t,i)}createEmbeddedViewImpl(t,i,r){let s=Al(this._declarationLView,this._declarationTContainer,t,{embeddedViewInjector:i,dehydratedView:r});return new Qs(s)}}return n})();function GE(){return kp(wn(),Ye())}function kp(n,e){return n.type&4?new sa(e,n,to(n,e)):null}function Rl(n,e,t,i,r){let s=n.data[e];if(s===null)s=WE(n,e,t,i,r),zv()&&(s.flags|=32);else if(s.type&64){s.type=t,s.value=i,s.attrs=r;let o=kv();s.injectorIndex=o===null?-1:o.injectorIndex}return qs(s,!0),s}function WE(n,e,t,i,r){let s=dh(),o=fh(),a=o?s:s&&s.parent,c=n.data[e]=$E(n,a,t,e,i,r);return jE(n,c,s,o),c}function jE(n,e,t,i){n.firstChild===null&&(n.firstChild=e),t!==null&&(i?t.child==null&&e.parent!==null&&(t.child=e):t.next===null&&(t.next=e,e.prev=t))}function $E(n,e,t,i,r,s){let o=e?e.injectorIndex:-1,a=0;return Fv()&&(a|=128),{type:t,index:i,insertBeforeIndex:null,injectorIndex:o,directiveStart:-1,directiveEnd:-1,directiveStylingLast:-1,componentOffset:-1,controlDirectiveIndex:-1,customControlIndex:-1,propertyBindings:null,flags:a,providerIndexes:0,value:r,attrs:s,mergedAttrs:null,localNames:null,initialInputs:null,inputs:null,hostDirectiveInputs:null,outputs:null,hostDirectiveOutputs:null,directiveToIndex:null,tView:null,next:null,prev:null,projectionNext:null,child:null,parent:e,projection:null,styles:null,stylesWithoutHost:null,residualStyles:void 0,classes:null,classesWithoutHost:null,residualClasses:void 0,classBindings:0,styleBindings:0}}function qE(n){let e=n[Qf]??[],i=n[Wt][wt],r=[];for(let s of e)s.data[s_]!==void 0?r.push(s):XE(s,i);n[Qf]=r}function XE(n,e){let t=0,i=n.firstChild;if(i){let r=n.data[r_];for(;t<r;){let s=i.nextSibling;__(e,i,!1),i=s,t++}}}var YE=()=>null,ZE=()=>null;function qh(n,e){return YE(n,e)}function Y_(n,e,t){return ZE(n,e,t)}var Z_=class{},Nl=class{},Xh=class{resolveComponentFactory(e){throw new $e(917,!1)}},Pl=class{static NULL=new Xh},cs=class{},Up=(()=>{class n{destroyNode=null;static __NG_ELEMENT_ID__=()=>JE()}return n})();function JE(){let n=Ye(),e=wn(),t=ni(e.index,n);return(Hi(t)?t:n)[wt]}var J_=(()=>{class n{static \u0275prov=at({token:n,providedIn:"root",factory:()=>null})}return n})();var rl={},Yh=class{injector;parentInjector;constructor(e,t){this.injector=e,this.parentInjector=t}get(e,t,i){let r=this.injector.get(e,rl,i);return r!==rl||t===rl?r:this.parentInjector.get(e,t,i)}};function hl(n,e,t){let i=t?n.styles:null,r=t?n.classes:null,s=0;if(e!==null)for(let o=0;o<e.length;o++){let a=e[o];if(typeof a=="number")s=a;else if(s==1)r=kf(r,a);else if(s==2){let c=a,l=e[++o];i=kf(i,c+": "+l+";")}}t?n.styles=i:n.stylesWithoutHost=i,t?n.classes=r:n.classesWithoutHost=r}function Bp(n,e=0){let t=Ye();if(t===null)return rt(n,e);let i=wn();return Yy(i,t,En(n),e)}function KE(n,e,t,i,r){let s=i===null?null:{"":-1},o=r(n,t);if(o!==null){let a=o,c=null,l=null;for(let u of o)if(u.resolveHostDirectives!==null){[a,c,l]=u.resolveHostDirectives(o);break}tT(n,e,t,a,s,c,l)}s!==null&&i!==null&&QE(t,i,s)}function QE(n,e,t){let i=n.localNames=[];for(let r=0;r<e.length;r+=2){let s=t[e[r+1]];if(s==null)throw new $e(-301,!1);i.push(e[r],s)}}function eT(n,e,t){e.componentOffset=t,(n.components??=[]).push(e.index)}function tT(n,e,t,i,r,s,o){let a=i.length,c=null;for(let d=0;d<a;d++){let h=i[d];c===null&&ns(h)&&(c=h,eT(n,t,d)),Jb(jy(t,e),n,h.type)}aT(t,n.data.length,a),c?.viewProvidersResolver&&c.viewProvidersResolver(c);for(let d=0;d<a;d++){let h=i[d];h.providersResolver&&h.providersResolver(h)}let l=!1,u=!1,f=E_(n,e,a,null);a>0&&(t.directiveToIndex=new Map);for(let d=0;d<a;d++){let h=i[d];if(t.mergedAttrs=xl(t.mergedAttrs,h.hostAttrs),iT(n,t,e,f,h),oT(f,h,r),o!==null&&o.has(h)){let[x,m]=o.get(h);t.directiveToIndex.set(h.type,[f,x+t.directiveStart,m+t.directiveStart])}else(s===null||!s.has(h))&&t.directiveToIndex.set(h.type,f);h.contentQueries!==null&&(t.flags|=4),(h.hostBindings!==null||h.hostAttrs!==null||h.hostVars!==0)&&(t.flags|=64);let g=h.type.prototype;!l&&(g.ngOnChanges||g.ngOnInit||g.ngDoCheck)&&((n.preOrderHooks??=[]).push(t.index),l=!0),!u&&(g.ngOnChanges||g.ngDoCheck)&&((n.preOrderCheckHooks??=[]).push(t.index),u=!0),f++}nT(n,t,s)}function nT(n,e,t){for(let i=e.directiveStart;i<e.directiveEnd;i++){let r=n.data[i];if(t===null||!t.has(r))Ey(0,e,r,i),Ey(1,e,r,i),wy(e,i,!1);else{let s=t.get(r);Ty(0,e,s,i),Ty(1,e,s,i),wy(e,i,!0)}}}function Ey(n,e,t,i){let r=n===0?t.inputs:t.outputs;for(let s in r)if(r.hasOwnProperty(s)){let o;n===0?o=e.inputs??={}:o=e.outputs??={},o[s]??=[],o[s].push(i),K_(e,s)}}function Ty(n,e,t,i){let r=n===0?t.inputs:t.outputs;for(let s in r)if(r.hasOwnProperty(s)){let o=r[s],a;n===0?a=e.hostDirectiveInputs??={}:a=e.hostDirectiveOutputs??={},a[o]??=[],a[o].push(i,s),K_(e,o)}}function K_(n,e){e==="class"?n.flags|=8:e==="style"&&(n.flags|=16)}function wy(n,e,t){let{attrs:i,inputs:r,hostDirectiveInputs:s}=n;if(i===null||!t&&r===null||t&&s===null||Ep(n)){n.initialInputs??=[],n.initialInputs.push(null);return}let o=null,a=0;for(;a<i.length;){let c=i[a];if(c===0){a+=4;continue}else if(c===5){a+=2;continue}else if(typeof c=="number")break;if(!t&&r.hasOwnProperty(c)){let l=r[c];for(let u of l)if(u===e){o??=[],o.push(c,i[a+1]);break}}else if(t&&s.hasOwnProperty(c)){let l=s[c];for(let u=0;u<l.length;u+=2)if(l[u]===e){o??=[],o.push(l[u+1],i[a+1]);break}}a+=2}n.initialInputs??=[],n.initialInputs.push(o)}function iT(n,e,t,i,r){n.data[i]=r;let s=r.factory||(r.factory=jr(r.type,!0)),o=new ta(s,ns(r),Bp,null);n.blueprint[i]=o,t[i]=o,rT(n,e,i,E_(n,t,r.hostVars,zn),r)}function rT(n,e,t,i,r){let s=r.hostBindings;if(s){let o=n.hostBindingOpCodes;o===null&&(o=n.hostBindingOpCodes=[]);let a=~e.index;sT(o)!=a&&o.push(a),o.push(t,i,s)}}function sT(n){let e=n.length;for(;e>0;){let t=n[--e];if(typeof t=="number"&&t<0)return t}return 0}function oT(n,e,t){if(t){if(e.exportAs)for(let i=0;i<e.exportAs.length;i++)t[e.exportAs[i]]=n;ns(e)&&(t[""]=n)}}function aT(n,e,t){n.flags|=1,n.directiveStart=e,n.directiveEnd=e+t,n.providerIndexes=e}function Q_(n,e,t,i,r,s,o,a){let c=e[Ce],l=c.consts,u=yi(l,o),f=Rl(c,n,t,i,u);return s&&KE(c,e,f,yi(l,a),r),f.mergedAttrs=xl(f.mergedAttrs,f.attrs),f.attrs!==null&&hl(f,f.attrs,!1),f.mergedAttrs!==null&&hl(f,f.mergedAttrs,!0),c.queries!==null&&c.queries.elementStart(c,f),f}function ex(n,e){Vb(n,e),eh(e)&&n.queries.elementEnd(e)}function cT(n,e,t,i,r,s){let o=e.consts,a=yi(o,r),c=Rl(e,n,t,i,a);if(c.mergedAttrs=xl(c.mergedAttrs,c.attrs),s!=null){let l=yi(o,s);c.localNames=[];for(let u=0;u<l.length;u+=2)c.localNames.push(l[u],-1)}return c.attrs!==null&&hl(c,c.attrs,!1),c.mergedAttrs!==null&&hl(c,c.mergedAttrs,!0),e.queries!==null&&e.queries.elementStart(e,c),c}function lT(n,e,t){return n[e]=t}function xi(n,e,t){if(t===zn)return!1;let i=n[e];return Object.is(i,t)?!1:(n[e]=t,!0)}function uT(n,e,t,i){let r=xi(n,e,t);return xi(n,e+1,i)||r}function sl(n,e,t){return function i(r){let s=mr(n)?ni(n.index,e):e;Fp(s,5);let o=e[Ft],a=Cy(e,o,t,r),c=i.__ngNextListenerFn__;for(;c;)a=Cy(e,o,c,r)&&a,c=c.__ngNextListenerFn__;return a}}function Cy(n,e,t,i){let r=Ae(null);try{return vt(ct.OutputStart,e,t),t(i)!==!1}catch(s){return wE(n,s),!1}finally{vt(ct.OutputEnd,e,t),Ae(r)}}function tx(n,e,t,i,r,s,o,a){let c=Uc(n),l=!1,u=null;if(!i&&c&&(u=fT(e,t,s,n.index)),u!==null){let f=u.__ngLastListenerFn__||u;f.__ngNextListenerFn__=o,u.__ngLastListenerFn__=o,l=!0}else{let f=ti(n,t),d=i?i(f):f;uS(t,d,s,a);let h=r.listen(d,s,a);if(!dT(s)){let g=i?x=>i(Hn(x[n.index])):n.index;nx(g,e,t,s,a,h,!1)}}return l}function dT(n){return n.startsWith("animation")||n.startsWith("transition")}function fT(n,e,t,i){let r=n.cleanup;if(r!=null)for(let s=0;s<r.length-1;s+=2){let o=r[s];if(o===t&&r[s+1]===i){let a=e[Gs],c=r[s+2];return a&&a.length>c?a[c]:null}typeof o=="string"&&(s+=2)}return null}function nx(n,e,t,i,r,s,o){let a=e.firstCreatePass?ah(e):null,c=oh(t),l=c.length;c.push(r,s),a&&a.push(i,n,l,(l+1)*(o?-1:1))}function Iy(n,e,t,i,r,s){let o=e[t],a=e[Ce],l=a.data[t].outputs[i],f=o[l].subscribe(s);nx(n.index,a,e,r,s,f,!0)}var Zh=Symbol("BINDING");function ix(n){return n.debugInfo?.className||n.type.name||null}var Jh=class extends Pl{ngModule;constructor(e){super(),this.ngModule=e}resolveComponentFactory(e){let t=Yr(e);return new oa(t,this.ngModule)}};function hT(n){return Object.keys(n).map(e=>{let[t,i,r]=n[e],s={propName:t,templateName:e,isSignal:(i&wl.SignalBased)!==0};return r&&(s.transform=r),s})}function pT(n){return Object.keys(n).map(e=>({propName:n[e],templateName:e}))}function mT(n,e,t){let i=e instanceof Fn?e:e?.injector;return i&&n.getStandaloneInjector!==null&&(i=n.getStandaloneInjector(i)||i),i?new Yh(t,i):t}function gT(n){let e=n.get(cs,null);if(e===null)throw new $e(407,!1);let t=n.get(J_,null),i=n.get(qr,null),r=n.get(no,null,{optional:!0});return{rendererFactory:e,sanitizer:t,changeDetectionScheduler:i,ngReflect:!1,tracingService:r}}function vT(n,e){let t=rx(n);return v_(e,t,t==="svg"?Tv:t==="math"?wv:null)}function rx(n){return(n.selectors[0][0]||"div").toLowerCase()}var oa=class extends Nl{componentDef;ngModule;selector;componentType;ngContentSelectors;isBoundToModule;cachedInputs=null;cachedOutputs=null;get inputs(){return this.cachedInputs??=hT(this.componentDef.inputs),this.cachedInputs}get outputs(){return this.cachedOutputs??=pT(this.componentDef.outputs),this.cachedOutputs}constructor(e,t){super(),this.componentDef=e,this.ngModule=t,this.componentType=e.type,this.selector=GS(e.selectors),this.ngContentSelectors=e.ngContentSelectors??[],this.isBoundToModule=!!t}create(e,t,i,r,s,o){vt(ct.DynamicComponentStart);let a=Ae(null);try{let c=this.componentDef,l=mT(c,r||this.ngModule,e),u=gT(l),f=u.tracingService;return f&&f.componentCreate?f.componentCreate(ix(c),()=>this.createComponentRef(u,l,t,i,s,o)):this.createComponentRef(u,l,t,i,s,o)}finally{Ae(a)}}createComponentRef(e,t,i,r,s,o){let a=this.componentDef,c=yT(r,a,o,s),l=e.rendererFactory.createRenderer(null,a),u=r?hE(l,r,a.encapsulation,t):vT(a,l),f=o?.some(Ay)||s?.some(g=>typeof g!="function"&&g.bindings.some(Ay)),d=wp(null,c,null,512|S_(a),null,null,e,l,t,null,c_(u,t,!0));d[jt]=u,Wc(d);let h=null;try{let g=Q_(jt,d,2,"#host",()=>c.directiveRegistry,!0,0);x_(l,u,g),Ks(u,d),N_(c,d,g),u_(c,g,d),ex(c,g),i!==void 0&&xT(g,this.ngContentSelectors,i),h=ni(g.index,d),d[Ft]=h[Ft],Op(c,d,null)}catch(g){throw h!==null&&Nh(h),Nh(d),g}finally{vt(ct.DynamicComponentEnd),jc()}return new pl(this.componentType,d,!!f)}};function yT(n,e,t,i){let r=n?["ng-version","21.2.7"]:WS(e.selectors[0]),s=null,o=null,a=0;if(t)for(let u of t)a+=u[Zh].requiredVars,u.create&&(u.targetIdx=0,(s??=[]).push(u)),u.update&&(u.targetIdx=0,(o??=[]).push(u));if(i)for(let u=0;u<i.length;u++){let f=i[u];if(typeof f!="function")for(let d of f.bindings){a+=d[Zh].requiredVars;let h=u+1;d.create&&(d.targetIdx=h,(s??=[]).push(d)),d.update&&(d.targetIdx=h,(o??=[]).push(d))}}let c=[e];if(i)for(let u of i){let f=typeof u=="function"?u:u.type,d=zf(f);c.push(d)}return Tp(0,null,_T(s,o),1,a,c,null,null,null,[r],null)}function _T(n,e){return!n&&!e?null:t=>{if(t&1&&n)for(let i of n)i.create();if(t&2&&e)for(let i of e)i.update()}}function Ay(n){let e=n[Zh].kind;return e==="input"||e==="twoWay"}var pl=class extends Z_{_rootLView;_hasInputBindings;instance;hostView;changeDetectorRef;componentType;location;previousInputValues=null;_tNode;constructor(e,t,i){super(),this._rootLView=t,this._hasInputBindings=i,this._tNode=Bc(t[Ce],jt),this.location=to(this._tNode,t),this.instance=ni(this._tNode.index,t)[Ft],this.hostView=this.changeDetectorRef=new Qs(t,void 0),this.componentType=e}setInput(e,t){this._hasInputBindings;let i=this._tNode;if(this.previousInputValues??=new Map,this.previousInputValues.has(e)&&Object.is(this.previousInputValues.get(e),t))return;let r=this._rootLView,s=Lp(i,r[Ce],r,e,t);this.previousInputValues.set(e,t);let o=ni(i.index,r);Fp(o,1)}get injector(){return new os(this._tNode,this._rootLView)}destroy(){this.hostView.destroy()}onDestroy(e){this.hostView.onDestroy(e)}};function xT(n,e,t){let i=n.projection=[];for(let r=0;r<e.length;r++){let s=t[r];i.push(s!=null&&s.length?Array.from(s):null)}}var Ll=(()=>{class n{static __NG_ELEMENT_ID__=MT}return n})();function MT(){let n=wn();return sx(n,Ye())}var Kh=class n extends Ll{_lContainer;_hostTNode;_hostLView;constructor(e,t,i){super(),this._lContainer=e,this._hostTNode=t,this._hostLView=i}get element(){return to(this._hostTNode,this._hostLView)}get injector(){return new os(this._hostTNode,this._hostLView)}get parentInjector(){let e=mp(this._hostTNode,this._hostLView);if(zy(e)){let t=cl(e,this._hostLView),i=al(e),r=t[Ce].data[i+8];return new os(r,t)}else return new os(null,this._hostLView)}clear(){for(;this.length>0;)this.remove(this.length-1)}get(e){let t=Dy(this._lContainer);return t!==null&&t[e]||null}get length(){return this._lContainer.length-At}createEmbeddedView(e,t,i){let r,s;typeof i=="number"?r=i:i!=null&&(r=i.index,s=i.injector);let o=qh(this._lContainer,e.ssrId),a=e.createEmbeddedViewImpl(t||{},s,o);return this.insertImpl(a,r,na(this._hostTNode,o)),a}createComponent(e,t,i,r,s,o,a){let c=e&&!Lb(e),l;if(c)l=t;else{let m=t||{};l=m.index,i=m.injector,r=m.projectableNodes,s=m.environmentInjector||m.ngModuleRef,o=m.directives,a=m.bindings}let u=c?e:new oa(Yr(e)),f=i||this.parentInjector;if(!s&&u.ngModule==null){let p=(c?f:this.parentInjector).get(Fn,null);p&&(s=p)}let d=Yr(u.componentType??{}),h=qh(this._lContainer,d?.id??null),g=h?.firstChild??null,x=u.create(f,r,g,s,o,a);return this.insertImpl(x.hostView,l,na(this._hostTNode,h)),x}insert(e,t){return this.insertImpl(e,t,!0)}insertImpl(e,t,i){let r=e._lView;if(Av(r)){let a=this.indexOf(e);if(a!==-1)this.detach(a);else{let c=r[Wt],l=new n(c,c[Bn],c[Wt]);l.detach(l.indexOf(e))}}let s=this._adjustIndex(t),o=this._lContainer;return Dl(o,r,s,i),e.attachToViewContainerRef(),$f(Ch(o),s,e),e}move(e,t){return this.insert(e,t)}indexOf(e){let t=Dy(this._lContainer);return t!==null?t.indexOf(e):-1}remove(e){let t=this._adjustIndex(e,-1),i=ra(this._lContainer,t);i&&(jo(Ch(this._lContainer),t),Cl(i[Ce],i))}detach(e){let t=this._adjustIndex(e,-1),i=ra(this._lContainer,t);return i&&jo(Ch(this._lContainer),t)!=null?new Qs(i):null}_adjustIndex(e,t=0){return e??this.length+t}};function Dy(n){return n[Yo]}function Ch(n){return n[Yo]||(n[Yo]=[])}function sx(n,e){let t,i=e[n.index];return ei(i)?t=i:(t=j_(i,e,null,n),e[n.index]=t,Cp(e,t)),ST(t,e,n,i),new Kh(t,n,e)}function bT(n,e){let t=n[wt],i=t.createComment(""),r=ti(e,n),s=t.parentNode(r);return fl(t,s,i,t.nextSibling(r),!1),i}var ST=wT,ET=()=>!1;function TT(n,e,t){return ET(n,e,t)}function wT(n,e,t,i){if(n[pr])return;let r;t.type&8?r=Hn(i):r=bT(e,t),n[pr]=r}var Qh=class n{queryList;matches=null;constructor(e){this.queryList=e}clone(){return new n(this.queryList)}setDirty(){this.queryList.setDirty()}},ep=class n{queries;constructor(e=[]){this.queries=e}createEmbeddedView(e){let t=e.queries;if(t!==null){let i=e.contentQueries!==null?e.contentQueries[0]:t.length,r=[];for(let s=0;s<i;s++){let o=t.getByIndex(s),a=this.queries[o.indexInDeclarationView];r.push(a.clone())}return new n(r)}return null}insertView(e){this.dirtyQueriesWithMatches(e)}detachView(e){this.dirtyQueriesWithMatches(e)}finishViewCreation(e){this.dirtyQueriesWithMatches(e)}dirtyQueriesWithMatches(e){for(let t=0;t<this.queries.length;t++)Vp(e,t).matches!==null&&this.queries[t].setDirty()}},tp=class{flags;read;predicate;constructor(e,t,i=null){this.flags=t,this.read=i,typeof e=="string"?this.predicate=LT(e):this.predicate=e}},np=class n{queries;constructor(e=[]){this.queries=e}elementStart(e,t){for(let i=0;i<this.queries.length;i++)this.queries[i].elementStart(e,t)}elementEnd(e){for(let t=0;t<this.queries.length;t++)this.queries[t].elementEnd(e)}embeddedTView(e){let t=null;for(let i=0;i<this.length;i++){let r=t!==null?t.length:0,s=this.getByIndex(i).embeddedTView(e,r);s&&(s.indexInDeclarationView=i,t!==null?t.push(s):t=[s])}return t!==null?new n(t):null}template(e,t){for(let i=0;i<this.queries.length;i++)this.queries[i].template(e,t)}getByIndex(e){return this.queries[e]}get length(){return this.queries.length}track(e){this.queries.push(e)}},ip=class n{metadata;matches=null;indexInDeclarationView=-1;crossesNgTemplate=!1;_declarationNodeIndex;_appliesToNextNode=!0;constructor(e,t=-1){this.metadata=e,this._declarationNodeIndex=t}elementStart(e,t){this.isApplyingToNode(t)&&this.matchTNode(e,t)}elementEnd(e){this._declarationNodeIndex===e.index&&(this._appliesToNextNode=!1)}template(e,t){this.elementStart(e,t)}embeddedTView(e,t){return this.isApplyingToNode(e)?(this.crossesNgTemplate=!0,this.addMatch(-e.index,t),new n(this.metadata)):null}isApplyingToNode(e){if(this._appliesToNextNode&&(this.metadata.flags&1)!==1){let t=this._declarationNodeIndex,i=e.parent;for(;i!==null&&i.type&8&&i.index!==t;)i=i.parent;return t===(i!==null?i.index:-1)}return this._appliesToNextNode}matchTNode(e,t){let i=this.metadata.predicate;if(Array.isArray(i))for(let r=0;r<i.length;r++){let s=i[r];this.matchTNodeWithReadOption(e,t,CT(t,s)),this.matchTNodeWithReadOption(e,t,il(t,e,s,!1,!1))}else i===sa?t.type&4&&this.matchTNodeWithReadOption(e,t,-1):this.matchTNodeWithReadOption(e,t,il(t,e,i,!1,!1))}matchTNodeWithReadOption(e,t,i){if(i!==null){let r=this.metadata.read;if(r!==null)if(r===us||r===Ll||r===sa&&t.type&4)this.addMatch(t.index,-2);else{let s=il(t,e,r,!1,!1);s!==null&&this.addMatch(t.index,s)}else this.addMatch(t.index,i)}}addMatch(e,t){this.matches===null?this.matches=[e,t]:this.matches.push(e,t)}};function CT(n,e){let t=n.localNames;if(t!==null){for(let i=0;i<t.length;i+=2)if(t[i]===e)return t[i+1]}return null}function IT(n,e){return n.type&11?to(n,e):n.type&4?kp(n,e):null}function AT(n,e,t,i){return t===-1?IT(e,n):t===-2?DT(n,e,i):ul(n,n[Ce],t,e)}function DT(n,e,t){if(t===us)return to(e,n);if(t===sa)return kp(e,n);if(t===Ll)return sx(e,n)}function ox(n,e,t,i){let r=e[vi].queries[i];if(r.matches===null){let s=n.data,o=t.matches,a=[];for(let c=0;o!==null&&c<o.length;c+=2){let l=o[c];if(l<0)a.push(null);else{let u=s[l];a.push(AT(e,u,o[c+1],t.metadata.read))}}r.matches=a}return r.matches}function rp(n,e,t,i){let r=n.queries.getByIndex(t),s=r.matches;if(s!==null){let o=ox(n,e,r,t);for(let a=0;a<s.length;a+=2){let c=s[a];if(c>0)i.push(o[a/2]);else{let l=s[a+1],u=e[-c];for(let f=At;f<u.length;f++){let d=u[f];d[fr]===d[Wt]&&rp(d[Ce],d,l,i)}if(u[ts]!==null){let f=u[ts];for(let d=0;d<f.length;d++){let h=f[d];rp(h[Ce],h,l,i)}}}}}return i}function RT(n,e){return n[vi].queries[e].queryList}function NT(n,e,t){let i=new dl((t&4)===4);return Nv(n,e,i,i.destroy),(e[vi]??=new ep).queries.push(new Qh(i))-1}function PT(n,e,t){let i=sn();return i.firstCreatePass&&(OT(i,new tp(n,e,t),-1),(e&2)===2&&(i.staticViewQueries=!0)),NT(i,Ye(),e)}function LT(n){return n.split(",").map(e=>e.trim())}function OT(n,e,t){n.queries===null&&(n.queries=new np),n.queries.track(new ip(e,t))}function Vp(n,e){return n.queries.getByIndex(e)}function FT(n,e){let t=n[Ce],i=Vp(t,e);return i.crossesNgTemplate?rp(t,n,e,[]):ox(t,n,i,e)}var ml=class{};var aa=class extends ml{injector;componentFactoryResolver=new Jh(this);instance=null;constructor(e){super();let t=new $r([...e.providers,{provide:ml,useValue:this},{provide:Pl,useValue:this.componentFactoryResolver}],e.parent||Xo(),e.debugName,new Set(["environment"]));this.injector=t,e.runEnvironmentInitializers&&t.resolveInjectorInitializers()}destroy(){this.injector.destroy()}onDestroy(e){this.injector.onDestroy(e)}};function ax(n,e,t=null){return new aa({providers:n,parent:e,debugName:t,runEnvironmentInitializers:!0}).injector}var kT=(()=>{class n{_injector;cachedInjectors=new Map;constructor(t){this._injector=t}getOrCreateStandaloneInjector(t){if(!t.standalone)return null;if(!this.cachedInjectors.has(t)){let i=Yf(!1,t.type),r=i.length>0?ax([i],this._injector,""):null;this.cachedInjectors.set(t,r)}return this.cachedInjectors.get(t)}ngOnDestroy(){try{for(let t of this.cachedInjectors.values())t!==null&&t.destroy()}finally{this.cachedInjectors.clear()}}static \u0275prov=at({token:n,providedIn:"environment",factory:()=>new n(rt(Fn))})}return n})();function Et(n){return pp(()=>{let e=cx(n),t=Gt(It({},e),{decls:n.decls,vars:n.vars,template:n.template,consts:n.consts||null,ngContentSelectors:n.ngContentSelectors,onPush:n.changeDetection===gp.OnPush,directiveDefs:null,pipeDefs:null,dependencies:e.standalone&&n.dependencies||null,getStandaloneInjector:e.standalone?r=>r.get(kT).getOrCreateStandaloneInjector(t):null,getExternalStyles:null,signals:n.signals??!1,data:n.data||{},encapsulation:n.encapsulation||ri.Emulated,styles:n.styles||cr,_:null,schemas:n.schemas||null,tView:null,id:""});e.standalone&&io("NgStandalone"),lx(t);let i=n.dependencies;return t.directiveDefs=Ry(i,UT),t.pipeDefs=Ry(i,uv),t.id=HT(t),t})}function UT(n){return Yr(n)||zf(n)}function BT(n,e){if(n==null)return Jr;let t={};for(let i in n)if(n.hasOwnProperty(i)){let r=n[i],s,o,a,c;Array.isArray(r)?(a=r[0],s=r[1],o=r[2]??s,c=r[3]||null):(s=r,o=r,a=wl.None,c=null),t[s]=[i,a,c],e[s]=o}return t}function VT(n){if(n==null)return Jr;let e={};for(let t in n)n.hasOwnProperty(t)&&(e[n[t]]=t);return e}function Hp(n){return pp(()=>{let e=cx(n);return lx(e),e})}function zp(n){return{type:n.type,name:n.name,factory:null,pure:n.pure!==!1,standalone:n.standalone??!0,onDestroy:n.type.prototype.ngOnDestroy||null}}function cx(n){let e={};return{type:n.type,providersResolver:null,viewProvidersResolver:null,factory:null,hostBindings:n.hostBindings||null,hostVars:n.hostVars||0,hostAttrs:n.hostAttrs||null,contentQueries:n.contentQueries||null,declaredInputs:e,inputConfig:n.inputs||Jr,exportAs:n.exportAs||null,standalone:n.standalone??!0,signals:n.signals===!0,selectors:n.selectors||cr,viewQuery:n.viewQuery||null,features:n.features||null,setInput:null,resolveHostDirectives:null,hostDirectives:null,controlDef:null,inputs:BT(n.inputs,e),outputs:VT(n.outputs),debugInfo:null}}function lx(n){n.features?.forEach(e=>e(n))}function Ry(n,e){return n?()=>{let t=typeof n=="function"?n():n,i=[];for(let r of t){let s=e(r);s!==null&&i.push(s)}return i}:null}function HT(n){let e=0,t=typeof n.consts=="function"?"":n.consts,i=[n.selectors,n.ngContentSelectors,n.hostVars,n.hostAttrs,t,n.vars,n.decls,n.encapsulation,n.standalone,n.signals,n.exportAs,JSON.stringify(n.inputs),JSON.stringify(n.outputs),Object.getOwnPropertyNames(n.type.prototype),!!n.contentQueries,!!n.viewQuery];for(let s of i.join("|"))e=Math.imul(31,e)+s.charCodeAt(0)<<0;return e+=2147483648,"c"+e}function zT(n,e,t,i,r,s,o,a){if(t.firstCreatePass){n.mergedAttrs=xl(n.mergedAttrs,n.attrs);let u=n.tView=Tp(2,n,r,s,o,t.directiveRegistry,t.pipeRegistry,null,t.schemas,t.consts,null);t.queries!==null&&(t.queries.template(t,n),u.queries=t.queries.embeddedTView(n))}a&&(n.flags|=a),qs(n,!1);let c=GT(t,e,n,i);qc()&&Rp(t,e,c,n),Ks(c,e);let l=j_(c,e,c,n);e[i+jt]=l,Cp(e,l),TT(l,n,e)}function gl(n,e,t,i,r,s,o,a,c,l,u){let f=t+jt,d;if(e.firstCreatePass){if(d=Rl(e,f,4,o||null,a||null),l!=null){let h=yi(e.consts,l);d.localNames=[];for(let g=0;g<h.length;g+=2)d.localNames.push(h[g],-1)}}else d=e.data[f];return zT(d,n,e,t,i,r,s,c),l!=null&&Pp(n,d,u),d}var GT=WT;function WT(n,e,t,i){return Xc(!0),e[wt].createComment("")}var Gp=new it("");function Wp(n){return!!n&&typeof n.then=="function"}function ux(n){return!!n&&typeof n.subscribe=="function"}var dx=new it("");var jp=(()=>{class n{resolve;reject;initialized=!1;done=!1;donePromise=new Promise((t,i)=>{this.resolve=t,this.reject=i});appInits=me(dx,{optional:!0})??[];injector=me(gi);constructor(){}runInitializers(){if(this.initialized)return;let t=[];for(let r of this.appInits){let s=Oc(this.injector,r);if(Wp(s))t.push(s);else if(ux(s)){let o=new Promise((a,c)=>{s.subscribe({complete:a,error:c})});t.push(o)}}let i=()=>{this.done=!0,this.resolve()};Promise.all(t).then(()=>{i()}).catch(r=>{this.reject(r)}),t.length===0&&i(),this.initialized=!0}static \u0275fac=function(i){return new(i||n)};static \u0275prov=at({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})(),fx=new it("");function hx(){df(()=>{let n="";throw new $e(600,n)})}function px(n){return n.isBoundToModule}var jT=10;var fa=(()=>{class n{_runningTick=!1;_destroyed=!1;_destroyListeners=[];_views=[];internalErrorHandler=me(ss);afterRenderManager=me(w_);zonelessEnabled=me(Jo);rootEffectScheduler=me(Zc);dirtyFlags=0;tracingSnapshot=null;allTestViews=new Set;autoDetectTestViews=new Set;includeAllTestViews=!1;afterTick=new Oi;get allViews(){return[...(this.includeAllTestViews?this.allTestViews:this.autoDetectTestViews).keys(),...this._views]}get destroyed(){return this._destroyed}componentTypes=[];components=[];internalPendingTask=me(Ys);get isStable(){return this.internalPendingTask.hasPendingTasksObservable.pipe(bf(t=>!t))}constructor(){me(no,{optional:!0})}whenStable(){let t;return new Promise(i=>{t=this.isStable.subscribe({next:r=>{r&&i()}})}).finally(()=>{t.unsubscribe()})}_injector=me(Fn);_rendererFactory=null;get injector(){return this._injector}bootstrap(t,i){return this.bootstrapImpl(t,i)}bootstrapImpl(t,i,r=gi.NULL){return this._injector.get(kn).run(()=>{vt(ct.BootstrapComponentStart);let o=t instanceof Nl;if(!this._injector.get(jp).done){let g="";throw new $e(405,g)}let c;o?c=t:c=this._injector.get(Pl).resolveComponentFactory(t),this.componentTypes.push(c.componentType);let l=px(c)?void 0:this._injector.get(ml),u=i||c.selector,f=c.create(r,[],u,l),d=f.location.nativeElement,h=f.injector.get(Gp,null);return h?.registerApplication(d),f.onDestroy(()=>{this.detachView(f.hostView),ea(this.components,f),h?.unregisterApplication(d)}),this._loadComponent(f),vt(ct.BootstrapComponentEnd,f),f})}tick(){this.zonelessEnabled||(this.dirtyFlags|=1),this._tick()}_tick(){vt(ct.ChangeDetectionStart),this.tracingSnapshot!==null?this.tracingSnapshot.run(Ap.CHANGE_DETECTION,this.tickImpl):this.tickImpl()}tickImpl=()=>{if(this._runningTick)throw vt(ct.ChangeDetectionEnd),new $e(101,!1);let t=Ae(null);try{this._runningTick=!0,this.synchronize()}finally{this._runningTick=!1,this.tracingSnapshot?.dispose(),this.tracingSnapshot=null,Ae(t),this.afterTick.next(),vt(ct.ChangeDetectionEnd)}};synchronize(){this._rendererFactory===null&&!this._injector.destroyed&&(this._rendererFactory=this._injector.get(cs,null,{optional:!0}));let t=0;for(;this.dirtyFlags!==0&&t++<jT;){vt(ct.ChangeDetectionSyncStart);try{this.synchronizeOnce()}finally{vt(ct.ChangeDetectionSyncEnd)}}}synchronizeOnce(){this.dirtyFlags&16&&(this.dirtyFlags&=-17,this.rootEffectScheduler.flush());let t=!1;if(this.dirtyFlags&7){let i=!!(this.dirtyFlags&1);this.dirtyFlags&=-8,this.dirtyFlags|=8;for(let{_lView:r}of this.allViews){if(!i&&!Zo(r))continue;let s=i&&!this.zonelessEnabled?0:1;H_(r,s),t=!0}if(this.dirtyFlags&=-5,this.syncDirtyFlagsWithViews(),this.dirtyFlags&23)return}t||(this._rendererFactory?.begin?.(),this._rendererFactory?.end?.()),this.dirtyFlags&8&&(this.dirtyFlags&=-9,this.afterRenderManager.execute()),this.syncDirtyFlagsWithViews()}syncDirtyFlagsWithViews(){if(this.allViews.some(({_lView:t})=>Zo(t))){this.dirtyFlags|=2;return}else this.dirtyFlags&=-8}attachView(t){let i=t;this._views.push(i),i.attachToAppRef(this)}detachView(t){let i=t;ea(this._views,i),i.detachFromAppRef()}_loadComponent(t){this.attachView(t.hostView);try{this.tick()}catch(r){this.internalErrorHandler(r)}this.components.push(t),this._injector.get(fx,[]).forEach(r=>r(t))}ngOnDestroy(){if(!this._destroyed)try{this._destroyListeners.forEach(t=>t()),this._views.slice().forEach(t=>t.destroy())}finally{this._destroyed=!0,this._views=[],this._destroyListeners=[]}}onDestroy(t){return this._destroyListeners.push(t),()=>ea(this._destroyListeners,t)}destroy(){if(this._destroyed)throw new $e(406,!1);let t=this._injector;t.destroy&&!t.destroyed&&t.destroy()}get viewCount(){return this._views.length}static \u0275fac=function(i){return new(i||n)};static \u0275prov=at({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();function ea(n,e){let t=n.indexOf(e);t>-1&&n.splice(t,1)}function ds(n,e,t,i){let r=Ye(),s=rs();if(xi(r,s,e)){let o=sn(),a=$c();SE(a,r,n,e,t,i)}return ds}var sp=class{destroy(e){}updateValue(e,t){}swap(e,t){let i=Math.min(e,t),r=Math.max(e,t),s=this.detach(r);if(r-i>1){let o=this.detach(i);this.attach(i,s),this.attach(r,o)}else this.attach(i,s)}move(e,t){this.attach(t,this.detach(e))}};function Ih(n,e,t,i,r){return n===t&&Object.is(e,i)?1:Object.is(r(n,e),r(t,i))?-1:0}function $T(n,e,t,i){let r,s,o=0,a=n.length-1,c=void 0;if(Array.isArray(e)){Ae(i);let l=e.length-1;for(Ae(null);o<=a&&o<=l;){let u=n.at(o),f=e[o],d=Ih(o,u,o,f,t);if(d!==0){d<0&&n.updateValue(o,f),o++;continue}let h=n.at(a),g=e[l],x=Ih(a,h,l,g,t);if(x!==0){x<0&&n.updateValue(a,g),a--,l--;continue}let m=t(o,u),p=t(a,h),M=t(o,f);if(Object.is(M,p)){let T=t(l,g);Object.is(T,m)?(n.swap(o,a),n.updateValue(a,g),l--,a--):n.move(a,o),n.updateValue(o,f),o++;continue}if(r??=new vl,s??=Py(n,o,a,t),op(n,r,o,M))n.updateValue(o,f),o++,a++;else if(s.has(M))r.set(m,n.detach(o)),a--;else{let T=n.create(o,e[o]);n.attach(o,T),o++,a++}}for(;o<=l;)Ny(n,r,t,o,e[o]),o++}else if(e!=null){Ae(i);let l=e[Symbol.iterator]();Ae(null);let u=l.next();for(;!u.done&&o<=a;){let f=n.at(o),d=u.value,h=Ih(o,f,o,d,t);if(h!==0)h<0&&n.updateValue(o,d),o++,u=l.next();else{r??=new vl,s??=Py(n,o,a,t);let g=t(o,d);if(op(n,r,o,g))n.updateValue(o,d),o++,a++,u=l.next();else if(!s.has(g))n.attach(o,n.create(o,d)),o++,a++,u=l.next();else{let x=t(o,f);r.set(x,n.detach(o)),a--}}}for(;!u.done;)Ny(n,r,t,n.length,u.value),u=l.next()}for(;o<=a;)n.destroy(n.detach(a--));r?.forEach(l=>{n.destroy(l)})}function op(n,e,t,i){return e!==void 0&&e.has(i)?(n.attach(t,e.get(i)),e.delete(i),!0):!1}function Ny(n,e,t,i,r){if(op(n,e,i,t(i,r)))n.updateValue(i,r);else{let s=n.create(i,r);n.attach(i,s)}}function Py(n,e,t,i){let r=new Set;for(let s=e;s<=t;s++)r.add(i(s,n.at(s)));return r}var vl=class{kvMap=new Map;_vMap=void 0;has(e){return this.kvMap.has(e)}delete(e){if(!this.has(e))return!1;let t=this.kvMap.get(e);return this._vMap!==void 0&&this._vMap.has(t)?(this.kvMap.set(e,this._vMap.get(t)),this._vMap.delete(t)):this.kvMap.delete(e),!0}get(e){return this.kvMap.get(e)}set(e,t){if(this.kvMap.has(e)){let i=this.kvMap.get(e);this._vMap===void 0&&(this._vMap=new Map);let r=this._vMap;for(;r.has(i);)i=r.get(i);r.set(i,t)}else this.kvMap.set(e,t)}forEach(e){for(let[t,i]of this.kvMap)if(e(i,t),this._vMap!==void 0){let r=this._vMap;for(;r.has(i);)i=r.get(i),e(i,t)}}};function si(n,e,t,i,r,s,o,a){io("NgControlFlow");let c=Ye(),l=sn(),u=yi(l.consts,s);return gl(c,l,n,e,t,i,r,u,256,o,a),$p}function $p(n,e,t,i,r,s,o,a){io("NgControlFlow");let c=Ye(),l=sn(),u=yi(l.consts,s);return gl(c,l,n,e,t,i,r,u,512,o,a),$p}function oi(n,e){io("NgControlFlow");let t=Ye(),i=rs(),r=t[i]!==zn?t[i]:-1,s=r!==-1?yl(t,jt+r):void 0,o=0;if(xi(t,i,n)){let a=Ae(null);try{if(s!==void 0&&q_(s,o),n!==-1){let c=jt+n,l=yl(t,c),u=up(t[Ce],c),f=Y_(l,u,t),d=Al(t,u,e,{dehydratedView:f});Dl(l,d,o,na(u,f))}}finally{Ae(a)}}else if(s!==void 0){let a=$_(s,o);a!==void 0&&(a[Ft]=e)}}var ap=class{lContainer;$implicit;$index;constructor(e,t,i){this.lContainer=e,this.$implicit=t,this.$index=i}get $count(){return this.lContainer.length-At}};function fs(n,e){return e}var cp=class{hasEmptyBlock;trackByFn;liveCollection;constructor(e,t,i){this.hasEmptyBlock=e,this.trackByFn=t,this.liveCollection=i}};function cn(n,e,t,i,r,s,o,a,c,l,u,f,d){io("NgControlFlow");let h=Ye(),g=sn(),x=c!==void 0,m=Ye(),p=a?o.bind(m[Vn][Ft]):o,M=new cp(x,p);m[jt+n]=M,gl(h,g,n+1,e,t,i,r,yi(g.consts,s),256),x&&gl(h,g,n+2,c,l,u,f,yi(g.consts,d),512)}var lp=class extends sp{lContainer;hostLView;templateTNode;operationsCounter=void 0;needsIndexUpdate=!1;constructor(e,t,i){super(),this.lContainer=e,this.hostLView=t,this.templateTNode=i}get length(){return this.lContainer.length-At}at(e){return this.getLView(e)[Ft].$implicit}attach(e,t){let i=t[zs];this.needsIndexUpdate||=e!==this.length,Dl(this.lContainer,t,e,na(this.templateTNode,i)),qT(this.lContainer,e)}detach(e){return this.needsIndexUpdate||=e!==this.length-1,XT(this.lContainer,e),YT(this.lContainer,e)}create(e,t){let i=qh(this.lContainer,this.templateTNode.tView.ssrId);return Al(this.hostLView,this.templateTNode,new ap(this.lContainer,t,e),{dehydratedView:i})}destroy(e){Cl(e[Ce],e)}updateValue(e,t){this.getLView(e)[Ft].$implicit=t}reset(){this.needsIndexUpdate=!1}updateIndexes(){if(this.needsIndexUpdate)for(let e=0;e<this.length;e++)this.getLView(e)[Ft].$index=e}getLView(e){return ZT(this.lContainer,e)}};function ln(n){let e=Ae(null),t=zi();try{let i=Ye(),r=i[Ce],s=i[t],o=t+1,a=yl(i,o);if(s.liveCollection===void 0){let l=up(r,o);s.liveCollection=new lp(a,i,l)}else s.liveCollection.reset();let c=s.liveCollection;if($T(c,n,s.trackByFn,e),c.updateIndexes(),s.hasEmptyBlock){let l=rs(),u=c.length===0;if(xi(i,l,u)){let f=t+2,d=yl(i,f);if(u){let h=up(r,f),g=Y_(d,h,i),x=Al(i,h,void 0,{dehydratedView:g});Dl(d,x,0,na(h,g))}else r.firstUpdatePass&&qE(d),q_(d,0)}}}finally{Ae(e)}}function yl(n,e){return n[e]}function qT(n,e){if(n.length<=At)return;let t=At+e,i=n[t],r=i?i[hr]:void 0;if(i&&r&&r.detachedLeaveAnimationFns&&r.detachedLeaveAnimationFns.length>0){let s=i[Bi];JS(s,r),as.delete(i[Vi]),r.detachedLeaveAnimationFns=void 0}}function XT(n,e){if(n.length<=At)return;let t=At+e,i=n[t],r=i?i[hr]:void 0;r&&r.leave&&r.leave.size>0&&(r.detachedLeaveAnimationFns=[])}function YT(n,e){return ra(n,e)}function ZT(n,e){return $_(n,e)}function up(n,e){return Bc(n,e)}function Ke(n,e,t){let i=Ye(),r=rs();if(xi(i,r,e)){let s=sn(),o=$c();vE(o,i,n,e,i[wt],t)}return Ke}function Ly(n,e,t,i,r){Lp(e,n,t,r?"class":"style",i)}function Oe(n,e,t,i){let r=Ye(),s=r[Ce],o=n+jt,a=s.firstCreatePass?Q_(o,r,2,e,bE,Ov(),t,i):s.data[o];if(mr(a)){let c=r[Qn].tracingService;if(c&&c.componentCreate){let l=s.data[a.directiveStart+a.componentOffset];return c.componentCreate(ix(l),()=>(Oy(n,e,r,a,i),Oe))}}return Oy(n,e,r,a,i),Oe}function Oy(n,e,t,i,r){if(L_(i,t,n,e,mx),Uc(i)){let s=t[Ce];N_(s,t,i),u_(s,i,t)}r!=null&&Pp(t,i)}function ke(){let n=sn(),e=wn(),t=O_(e);return n.firstCreatePass&&ex(n,t),lh(t)&&uh(),ch(),t.classesWithoutHost!=null&&zb(t)&&Ly(n,t,Ye(),t.classesWithoutHost,!0),t.stylesWithoutHost!=null&&Gb(t)&&Ly(n,t,Ye(),t.stylesWithoutHost,!1),ke}function st(n,e,t,i){return Oe(n,e,t,i),ke(),st}function kt(n,e,t,i){let r=Ye(),s=r[Ce],o=n+jt,a=s.firstCreatePass?cT(o,s,2,e,t,i):s.data[o];return L_(a,r,n,e,mx),i!=null&&Pp(r,a),kt}function $t(){let n=wn(),e=O_(n);return lh(e)&&uh(),ch(),$t}function Vt(n,e,t,i){return kt(n,e,t,i),$t(),Vt}var mx=(n,e,t,i,r)=>(Xc(!0),v_(e[wt],i,Zv()));function ro(){return Ye()}function en(n,e,t){let i=Ye(),r=rs();if(xi(i,r,e)){let s=sn(),o=$c();P_(o,i,n,e,i[wt],t)}return en}var ha="en-US";var JT=ha;function gx(n){typeof n=="string"&&(JT=n.toLowerCase().replace(/_/g,"-"))}function so(n,e,t){let i=Ye(),r=sn(),s=wn();return KT(r,i,i[wt],s,n,e,t),so}function _r(n,e,t){let i=Ye(),r=sn(),s=wn();return(s.type&3||t)&&tx(s,r,i,t,i[wt],n,e,sl(s,i,e)),_r}function KT(n,e,t,i,r,s,o){let a=!0,c=null;if((i.type&3||o)&&(c??=sl(i,e,s),tx(i,n,e,o,t,r,s,c)&&(a=!1)),a){let l=i.outputs?.[r],u=i.hostDirectiveOutputs?.[r];if(u&&u.length)for(let f=0;f<u.length;f+=2){let d=u[f],h=u[f+1];c??=sl(i,e,s),Iy(i,e,d,h,r,c)}if(l&&l.length)for(let f of l)c??=sl(i,e,s),Iy(i,e,f,r,r,c)}}function Gn(n=1){return Yv(n)}function Ol(n,e,t){return PT(n,e,t),Ol}function qp(n){let e=Ye(),t=sn(),i=mh();Gc(i+1);let r=Vp(t,i);if(n.dirty&&Iv(e)===((r.metadata.flags&2)===2)){if(r.matches===null)n.reset([]);else{let s=FT(e,i);n.reset(s,iS),n.notifyOnChanges()}return!0}return!1}function Xp(){return RT(Ye(),mh())}function el(n,e){return n<<17|e<<2}function ls(n){return n>>17&32767}function QT(n){return(n&2)==2}function ew(n,e){return n&131071|e<<17}function dp(n){return n|2}function eo(n){return(n&131068)>>2}function Ah(n,e){return n&-131069|e<<2}function tw(n){return(n&1)===1}function fp(n){return n|1}function nw(n,e,t,i,r,s){let o=s?e.classBindings:e.styleBindings,a=ls(o),c=eo(o);n[i]=t;let l=!1,u;if(Array.isArray(t)){let f=t;u=f[1],(u===null||Hs(f,u)>0)&&(l=!0)}else u=t;if(r)if(c!==0){let d=ls(n[a+1]);n[i+1]=el(d,a),d!==0&&(n[d+1]=Ah(n[d+1],i)),n[a+1]=ew(n[a+1],i)}else n[i+1]=el(a,0),a!==0&&(n[a+1]=Ah(n[a+1],i)),a=i;else n[i+1]=el(c,0),a===0?a=i:n[c+1]=Ah(n[c+1],i),c=i;l&&(n[i+1]=dp(n[i+1])),Fy(n,u,i,!0),Fy(n,u,i,!1),iw(e,u,n,i,s),o=el(a,c),s?e.classBindings=o:e.styleBindings=o}function iw(n,e,t,i,r){let s=r?n.residualClasses:n.residualStyles;s!=null&&typeof e=="string"&&Hs(s,e)>=0&&(t[i+1]=fp(t[i+1]))}function Fy(n,e,t,i){let r=n[t+1],s=e===null,o=i?ls(r):eo(r),a=!1;for(;o!==0&&(a===!1||s);){let c=n[o],l=n[o+1];rw(c,e)&&(a=!0,n[o+1]=i?fp(l):dp(l)),o=i?ls(l):eo(l)}a&&(n[t+1]=i?dp(r):fp(r))}function rw(n,e){return n===null||e==null||(Array.isArray(n)?n[1]:n)===e?!0:Array.isArray(n)&&typeof e=="string"?Hs(n,e)>=0:!1}function oo(n,e){return sw(n,e,null,!0),oo}function sw(n,e,t,i){let r=Ye(),s=sn(),o=ph(2);if(s.firstUpdatePass&&aw(s,n,o,i),e!==zn&&xi(r,o,e)){let a=s.data[zi()];fw(s,a,r,r[wt],n,r[o+1]=hw(e,t),i,o)}}function ow(n,e){return e>=n.expandoStartIndex}function aw(n,e,t,i){let r=n.data;if(r[t+1]===null){let s=r[zi()],o=ow(n,t);pw(s,i)&&e===null&&!o&&(e=!1),e=cw(r,s,e,i),nw(r,s,e,t,o,i)}}function cw(n,e,t,i){let r=jv(n),s=i?e.residualClasses:e.residualStyles;if(r===null)(i?e.classBindings:e.styleBindings)===0&&(t=Dh(null,n,e,t,i),t=ca(t,e.attrs,i),s=null);else{let o=e.directiveStylingLast;if(o===-1||n[o]!==r)if(t=Dh(r,n,e,t,i),s===null){let c=lw(n,e,i);c!==void 0&&Array.isArray(c)&&(c=Dh(null,n,e,c[1],i),c=ca(c,e.attrs,i),uw(n,e,i,c))}else s=dw(n,e,i)}return s!==void 0&&(i?e.residualClasses=s:e.residualStyles=s),t}function lw(n,e,t){let i=t?e.classBindings:e.styleBindings;if(eo(i)!==0)return n[ls(i)]}function uw(n,e,t,i){let r=t?e.classBindings:e.styleBindings;n[ls(r)]=i}function dw(n,e,t){let i,r=e.directiveEnd;for(let s=1+e.directiveStylingLast;s<r;s++){let o=n[s].hostAttrs;i=ca(i,o,t)}return ca(i,e.attrs,t)}function Dh(n,e,t,i,r){let s=null,o=t.directiveEnd,a=t.directiveStylingLast;for(a===-1?a=t.directiveStart:a++;a<o&&(s=e[a],i=ca(i,s.hostAttrs,r),s!==n);)a++;return n!==null&&(t.directiveStylingLast=a),i}function ca(n,e,t){let i=t?1:2,r=-1;if(e!==null)for(let s=0;s<e.length;s++){let o=e[s];typeof o=="number"?r=o:r===i&&(Array.isArray(n)||(n=n===void 0?[]:["",n]),vv(n,o,t?!0:e[++s]))}return n===void 0?null:n}function fw(n,e,t,i,r,s,o,a){if(!(e.type&3))return;let c=n.data,l=c[a+1],u=tw(l)?ky(c,e,t,r,eo(l),o):void 0;if(!_l(u)){_l(s)||QT(l)&&(s=ky(c,null,t,r,a,o));let f=th(zi(),t);fE(i,o,f,r,s)}}function ky(n,e,t,i,r,s){let o=e===null,a;for(;r>0;){let c=n[r],l=Array.isArray(c),u=l?c[1]:c,f=u===null,d=t[r+1];d===zn&&(d=f?cr:void 0);let h=f?Lc(d,i):u===i?d:void 0;if(l&&!_l(h)&&(h=Lc(c,i)),_l(h)&&(a=h,o))return a;let g=n[r+1];r=o?ls(g):eo(g)}if(e!==null){let c=s?e.residualClasses:e.residualStyles;c!=null&&(a=Lc(c,i))}return a}function _l(n){return n!==void 0}function hw(n,e){return n==null||n===""||(typeof e=="string"?n=n+e:typeof n=="object"&&(n=Ic(Mi(n)))),n}function pw(n,e){return(n.flags&(e?8:16))!==0}function bt(n,e=""){let t=Ye(),i=sn(),r=n+jt,s=i.firstCreatePass?Rl(i,r,1,e,null):i.data[r],o=mw(i,t,s,e);t[r]=o,qc()&&Rp(i,t,o,s),qs(s,!1)}var mw=(n,e,t,i)=>(Xc(!0),DS(e[wt],i));function gw(n,e,t,i=""){return xi(n,rs(),t)?e+Zr(t)+i:zn}function vw(n,e,t,i,r,s=""){let o=Vv(),a=uT(n,o,t,r);return ph(2),a?e+Zr(t)+i+Zr(r)+s:zn}function Wn(n){return pa("",n),Wn}function pa(n,e,t){let i=Ye(),r=gw(i,n,e,t);return r!==zn&&vx(i,zi(),r),pa}function Fl(n,e,t,i,r){let s=Ye(),o=vw(s,n,e,t,i,r);return o!==zn&&vx(s,zi(),o),Fl}function vx(n,e,t){let i=th(e,n);RS(n[wt],i,t)}function yw(n,e){let t=n[e];return t===zn?void 0:t}function _w(n,e,t,i,r,s){let o=e+t;return xi(n,o,r)?lT(n,o+1,s?i.call(s,r):i(r)):yw(n,o+1)}function Qe(n,e){let t=sn(),i,r=n+jt;t.firstCreatePass?(i=xw(e,t.pipeRegistry),t.data[r]=i,i.onDestroy&&(t.destroyHooks??=[]).push(r,i.onDestroy)):i=t.data[r];let s=i.factory||(i.factory=jr(i.type,!0)),o,a=gn(Bp);try{let c=ll(!1),l=s();return ll(c),nh(t,Ye(),r,l),l}finally{gn(a)}}function xw(n,e){if(e)for(let t=e.length-1;t>=0;t--){let i=e[t];if(n===i.name)return i}}function et(n,e,t){let i=n+jt,r=Ye(),s=Cv(r,i);return Mw(r,i)?_w(r,Bv(),e,s.transform,t,s):s.transform(t)}function Mw(n,e){return n[Ce].data[e].pure}var yx=(()=>{class n{applicationErrorHandler=me(ss);appRef=me(fa);taskService=me(Ys);ngZone=me(kn);zonelessEnabled=me(Jo);tracing=me(no,{optional:!0});zoneIsDefined=typeof Zone<"u"&&!!Zone.root.run;schedulerTickApplyArgs=[{data:{__scheduler_tick__:!0}}];subscriptions=new mn;angularZoneId=this.zoneIsDefined?this.ngZone._inner?.get(Go):null;scheduleInRootZone=!this.zonelessEnabled&&this.zoneIsDefined&&(me(bh,{optional:!0})??!1);cancelScheduledCallback=null;useMicrotaskScheduler=!1;runningTick=!1;pendingRenderTaskId=null;constructor(){this.subscriptions.add(this.appRef.afterTick.subscribe(()=>{let t=this.taskService.add();if(!this.runningTick&&(this.cleanup(),!this.zonelessEnabled||this.appRef.includeAllTestViews)){this.taskService.remove(t);return}this.switchToMicrotaskScheduler(),this.taskService.remove(t)})),this.subscriptions.add(this.ngZone.onUnstable.subscribe(()=>{this.runningTick||this.cleanup()}))}switchToMicrotaskScheduler(){this.ngZone.runOutsideAngular(()=>{let t=this.taskService.add();this.useMicrotaskScheduler=!0,queueMicrotask(()=>{this.useMicrotaskScheduler=!1,this.taskService.remove(t)})})}notify(t){if(!this.zonelessEnabled&&t===5)return;switch(t){case 0:{this.appRef.dirtyFlags|=2;break}case 3:case 2:case 4:case 5:case 1:{this.appRef.dirtyFlags|=4;break}case 6:{this.appRef.dirtyFlags|=2;break}case 12:{this.appRef.dirtyFlags|=16;break}case 13:{this.appRef.dirtyFlags|=2;break}case 11:break;default:this.appRef.dirtyFlags|=8}if(this.appRef.tracingSnapshot=this.tracing?.snapshot(this.appRef.tracingSnapshot)??null,!this.shouldScheduleTick())return;let i=this.useMicrotaskScheduler?ty:yh;this.pendingRenderTaskId=this.taskService.add(),this.scheduleInRootZone?this.cancelScheduledCallback=Zone.root.run(()=>i(()=>this.tick())):this.cancelScheduledCallback=this.ngZone.runOutsideAngular(()=>i(()=>this.tick()))}shouldScheduleTick(){return!(this.appRef.destroyed||this.pendingRenderTaskId!==null||this.runningTick||this.appRef._runningTick||!this.zonelessEnabled&&this.zoneIsDefined&&Zone.current.get(Go+this.angularZoneId))}tick(){if(this.runningTick||this.appRef.destroyed)return;if(this.appRef.dirtyFlags===0){this.cleanup();return}!this.zonelessEnabled&&this.appRef.dirtyFlags&7&&(this.appRef.dirtyFlags|=1);let t=this.taskService.add();try{this.ngZone.run(()=>{this.runningTick=!0,this.appRef._tick()},void 0,this.schedulerTickApplyArgs)}catch(i){this.applicationErrorHandler(i)}finally{this.taskService.remove(t),this.cleanup()}}ngOnDestroy(){this.subscriptions.unsubscribe(),this.cleanup()}cleanup(){if(this.runningTick=!1,this.cancelScheduledCallback?.(),this.cancelScheduledCallback=null,this.pendingRenderTaskId!==null){let t=this.pendingRenderTaskId;this.pendingRenderTaskId=null,this.taskService.remove(t)}}static \u0275fac=function(i){return new(i||n)};static \u0275prov=at({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();function _x(){return[{provide:qr,useExisting:yx},{provide:kn,useClass:Wo},{provide:Jo,useValue:!0}]}function bw(){return typeof $localize<"u"&&$localize.locale||ha}var Yp=new it("",{factory:()=>me(Yp,{optional:!0,skipSelf:!0})||bw()});function xr(n,e){return dc(n,e?.equal)}var bx=Symbol("InputSignalNode#UNSET"),Rw=Gt(It({},fc),{transformFn:void 0,applyValueToInputSignal(n,e){ko(n,e)}});function Sx(n,e){let t=Object.create(Rw);t.value=n,t.transformFn=e?.transform;function i(){if(Lo(t),t.value===bx){let r=null;throw new $e(-950,r)}return t.value}return i[yn]=t,i}function xx(n,e){return Sx(n,e)}function Nw(n){return Sx(bx,n)}var ga=(xx.required=Nw,xx);var Zp=new it(""),Pw=new it("");function ma(n){return!n.moduleRef}function Lw(n){let e=ma(n)?n.r3Injector:n.moduleRef.injector,t=e.get(kn);return t.run(()=>{ma(n)?n.r3Injector.resolveInjectorInitializers():n.moduleRef.resolveInjectorInitializers();let i=e.get(ss),r;if(t.runOutsideAngular(()=>{r=t.onError.subscribe({next:i})}),ma(n)){let s=()=>e.destroy(),o=n.platformInjector.get(Zp);o.add(s),e.onDestroy(()=>{r.unsubscribe(),o.delete(s)})}else{let s=()=>n.moduleRef.destroy(),o=n.platformInjector.get(Zp);o.add(s),n.moduleRef.onDestroy(()=>{ea(n.allPlatformModules,n.moduleRef),r.unsubscribe(),o.delete(s)})}return Fw(i,t,()=>{let s=e.get(Ys),o=s.add(),a=e.get(jp);return a.runInitializers(),a.donePromise.then(()=>{let c=e.get(Yp,ha);if(gx(c||ha),!e.get(Pw,!0))return ma(n)?e.get(fa):(n.allPlatformModules.push(n.moduleRef),n.moduleRef);if(ma(n)){let u=e.get(fa);return n.rootComponent!==void 0&&u.bootstrap(n.rootComponent),u}else return Ow?.(n.moduleRef,n.allPlatformModules),n.moduleRef}).finally(()=>{s.remove(o)})})})}var Ow;function Fw(n,e,t){try{let i=t();return Wp(i)?i.catch(r=>{throw e.runOutsideAngular(()=>n(r)),r}):i}catch(i){throw e.runOutsideAngular(()=>n(i)),i}}var kl=null;function kw(n=[],e){return gi.create({name:e,providers:[{provide:qo,useValue:"platform"},{provide:Zp,useValue:new Set([()=>kl=null])},...n]})}function Uw(n=[]){if(kl)return kl;let e=kw(n);return kl=e,hx(),Bw(e),e}function Bw(n){let e=n.get(bl,null);Oc(n,()=>{e?.forEach(t=>t())})}var Vw=1e4;var DB=Vw-1e3;function Ex(n){let{rootComponent:e,appProviders:t,platformProviders:i,platformRef:r}=n;vt(ct.BootstrapApplicationStart);try{let s=r?.injector??Uw(i),o=[_x(),iy,...t||[]],a=new aa({providers:o,parent:s,debugName:"",runEnvironmentInitializers:!1});return Lw({r3Injector:a.injector,platformInjector:s,rootComponent:e})}catch(s){return Promise.reject(s)}finally{vt(ct.BootstrapApplicationEnd)}}var Tx=null;function hs(){return Tx}function Jp(n){Tx??=n}var va=class{};function Kp(n,e){e=encodeURIComponent(e);for(let t of n.split(";")){let i=t.indexOf("="),[r,s]=i==-1?[t,""]:[t.slice(0,i),t.slice(i+1)];if(r.trim()===e)return decodeURIComponent(s)}return null}var ya=class{};var wx="browser";var _a=class{_doc;constructor(e){this._doc=e}manager},Ul=(()=>{class n extends _a{constructor(t){super(t)}supports(t){return!0}addEventListener(t,i,r,s){return t.addEventListener(i,r,s),()=>this.removeEventListener(t,i,r,s)}removeEventListener(t,i,r,s){return t.removeEventListener(i,r,s)}static \u0275fac=function(i){return new(i||n)(rt(on))};static \u0275prov=at({token:n,factory:n.\u0275fac})}return n})(),Hl=new it(""),nm=(()=>{class n{_zone;_plugins;_eventNameToPlugin=new Map;constructor(t,i){this._zone=i,t.forEach(o=>{o.manager=this});let r=t.filter(o=>!(o instanceof Ul));this._plugins=r.slice().reverse();let s=t.find(o=>o instanceof Ul);s&&this._plugins.push(s)}addEventListener(t,i,r,s){return this._findPluginFor(i).addEventListener(t,i,r,s)}getZone(){return this._zone}_findPluginFor(t){let i=this._eventNameToPlugin.get(t);if(i)return i;if(i=this._plugins.find(s=>s.supports(t)),!i)throw new $e(5101,!1);return this._eventNameToPlugin.set(t,i),i}static \u0275fac=function(i){return new(i||n)(rt(Hl),rt(kn))};static \u0275prov=at({token:n,factory:n.\u0275fac})}return n})(),Qp="ng-app-id";function Cx(n){for(let e of n)e.remove()}function Ix(n,e){let t=e.createElement("style");return t.textContent=n,t}function Hw(n,e,t,i){let r=n.head?.querySelectorAll(`style[${Qp}="${e}"],link[${Qp}="${e}"]`);if(r)for(let s of r)s.removeAttribute(Qp),s instanceof HTMLLinkElement?i.set(s.href.slice(s.href.lastIndexOf("/")+1),{usage:0,elements:[s]}):s.textContent&&t.set(s.textContent,{usage:0,elements:[s]})}function tm(n,e){let t=e.createElement("link");return t.setAttribute("rel","stylesheet"),t.setAttribute("href",n),t}var im=(()=>{class n{doc;appId;nonce;inline=new Map;external=new Map;hosts=new Set;constructor(t,i,r,s={}){this.doc=t,this.appId=i,this.nonce=r,Hw(t,i,this.inline,this.external),this.hosts.add(t.head)}addStyles(t,i){for(let r of t)this.addUsage(r,this.inline,Ix);i?.forEach(r=>this.addUsage(r,this.external,tm))}removeStyles(t,i){for(let r of t)this.removeUsage(r,this.inline);i?.forEach(r=>this.removeUsage(r,this.external))}addUsage(t,i,r){let s=i.get(t);s?s.usage++:i.set(t,{usage:1,elements:[...this.hosts].map(o=>this.addElement(o,r(t,this.doc)))})}removeUsage(t,i){let r=i.get(t);r&&(r.usage--,r.usage<=0&&(Cx(r.elements),i.delete(t)))}ngOnDestroy(){for(let[,{elements:t}]of[...this.inline,...this.external])Cx(t);this.hosts.clear()}addHost(t){this.hosts.add(t);for(let[i,{elements:r}]of this.inline)r.push(this.addElement(t,Ix(i,this.doc)));for(let[i,{elements:r}]of this.external)r.push(this.addElement(t,tm(i,this.doc)))}removeHost(t){this.hosts.delete(t)}addElement(t,i){return this.nonce&&i.setAttribute("nonce",this.nonce),t.appendChild(i)}static \u0275fac=function(i){return new(i||n)(rt(on),rt(Ml),rt(Sl,8),rt(la))};static \u0275prov=at({token:n,factory:n.\u0275fac})}return n})(),em={svg:"http://www.w3.org/2000/svg",xhtml:"http://www.w3.org/1999/xhtml",xlink:"http://www.w3.org/1999/xlink",xml:"http://www.w3.org/XML/1998/namespace",xmlns:"http://www.w3.org/2000/xmlns/",math:"http://www.w3.org/1998/Math/MathML"},rm=/%COMP%/g;var Dx="%COMP%",zw=`_nghost-${Dx}`,Gw=`_ngcontent-${Dx}`,Ww=!0,jw=new it("",{factory:()=>Ww});function $w(n){return Gw.replace(rm,n)}function qw(n){return zw.replace(rm,n)}function Rx(n,e){return e.map(t=>t.replace(rm,n))}var sm=(()=>{class n{eventManager;sharedStylesHost;appId;removeStylesOnCompDestroy;doc;ngZone;nonce;tracingService;rendererByCompId=new Map;defaultRenderer;constructor(t,i,r,s,o,a,c=null,l=null){this.eventManager=t,this.sharedStylesHost=i,this.appId=r,this.removeStylesOnCompDestroy=s,this.doc=o,this.ngZone=a,this.nonce=c,this.tracingService=l,this.defaultRenderer=new xa(t,o,a,this.tracingService)}createRenderer(t,i){if(!t||!i)return this.defaultRenderer;let r=this.getOrCreateRenderer(t,i);return r instanceof Vl?r.applyToHost(t):r instanceof Ma&&r.applyStyles(),r}getOrCreateRenderer(t,i){let r=this.rendererByCompId,s=r.get(i.id);if(!s){let o=this.doc,a=this.ngZone,c=this.eventManager,l=this.sharedStylesHost,u=this.removeStylesOnCompDestroy,f=this.tracingService;switch(i.encapsulation){case ri.Emulated:s=new Vl(c,l,i,this.appId,u,o,a,f);break;case ri.ShadowDom:return new Bl(c,t,i,o,a,this.nonce,f,l);case ri.ExperimentalIsolatedShadowDom:return new Bl(c,t,i,o,a,this.nonce,f);default:s=new Ma(c,l,i,u,o,a,f);break}r.set(i.id,s)}return s}ngOnDestroy(){this.rendererByCompId.clear()}componentReplaced(t){this.rendererByCompId.delete(t)}static \u0275fac=function(i){return new(i||n)(rt(nm),rt(im),rt(Ml),rt(jw),rt(on),rt(kn),rt(Sl),rt(no,8))};static \u0275prov=at({token:n,factory:n.\u0275fac})}return n})(),xa=class{eventManager;doc;ngZone;tracingService;data=Object.create(null);throwOnSyntheticProps=!0;constructor(e,t,i,r){this.eventManager=e,this.doc=t,this.ngZone=i,this.tracingService=r}destroy(){}destroyNode=null;createElement(e,t){return t?this.doc.createElementNS(em[t]||t,e):this.doc.createElement(e)}createComment(e){return this.doc.createComment(e)}createText(e){return this.doc.createTextNode(e)}appendChild(e,t){(Ax(e)?e.content:e).appendChild(t)}insertBefore(e,t,i){e&&(Ax(e)?e.content:e).insertBefore(t,i)}removeChild(e,t){t.remove()}selectRootElement(e,t){let i=typeof e=="string"?this.doc.querySelector(e):e;if(!i)throw new $e(-5104,!1);return t||(i.textContent=""),i}parentNode(e){return e.parentNode}nextSibling(e){return e.nextSibling}setAttribute(e,t,i,r){if(r){t=r+":"+t;let s=em[r];s?e.setAttributeNS(s,t,i):e.setAttribute(t,i)}else e.setAttribute(t,i)}removeAttribute(e,t,i){if(i){let r=em[i];r?e.removeAttributeNS(r,t):e.removeAttribute(`${i}:${t}`)}else e.removeAttribute(t)}addClass(e,t){e.classList.add(t)}removeClass(e,t){e.classList.remove(t)}setStyle(e,t,i,r){r&(Wi.DashCase|Wi.Important)?e.style.setProperty(t,i,r&Wi.Important?"important":""):e.style[t]=i}removeStyle(e,t,i){i&Wi.DashCase?e.style.removeProperty(t):e.style[t]=""}setProperty(e,t,i){e!=null&&(e[t]=i)}setValue(e,t){e.nodeValue=t}listen(e,t,i,r){if(typeof e=="string"&&(e=hs().getGlobalEventTarget(this.doc,e),!e))throw new $e(5102,!1);let s=this.decoratePreventDefault(i);return this.tracingService?.wrapEventListener&&(s=this.tracingService.wrapEventListener(e,t,s)),this.eventManager.addEventListener(e,t,s,r)}decoratePreventDefault(e){return t=>{if(t==="__ngUnwrap__")return e;e(t)===!1&&t.preventDefault()}}};function Ax(n){return n.tagName==="TEMPLATE"&&n.content!==void 0}var Bl=class extends xa{hostEl;sharedStylesHost;shadowRoot;constructor(e,t,i,r,s,o,a,c){super(e,r,s,a),this.hostEl=t,this.sharedStylesHost=c,this.shadowRoot=t.attachShadow({mode:"open"}),this.sharedStylesHost&&this.sharedStylesHost.addHost(this.shadowRoot);let l=i.styles;l=Rx(i.id,l);for(let f of l){let d=document.createElement("style");o&&d.setAttribute("nonce",o),d.textContent=f,this.shadowRoot.appendChild(d)}let u=i.getExternalStyles?.();if(u)for(let f of u){let d=tm(f,r);o&&d.setAttribute("nonce",o),this.shadowRoot.appendChild(d)}}nodeOrShadowRoot(e){return e===this.hostEl?this.shadowRoot:e}appendChild(e,t){return super.appendChild(this.nodeOrShadowRoot(e),t)}insertBefore(e,t,i){return super.insertBefore(this.nodeOrShadowRoot(e),t,i)}removeChild(e,t){return super.removeChild(null,t)}parentNode(e){return this.nodeOrShadowRoot(super.parentNode(this.nodeOrShadowRoot(e)))}destroy(){this.sharedStylesHost&&this.sharedStylesHost.removeHost(this.shadowRoot)}},Ma=class extends xa{sharedStylesHost;removeStylesOnCompDestroy;styles;styleUrls;constructor(e,t,i,r,s,o,a,c){super(e,s,o,a),this.sharedStylesHost=t,this.removeStylesOnCompDestroy=r;let l=i.styles;this.styles=c?Rx(c,l):l,this.styleUrls=i.getExternalStyles?.(c)}applyStyles(){this.sharedStylesHost.addStyles(this.styles,this.styleUrls)}destroy(){this.removeStylesOnCompDestroy&&as.size===0&&this.sharedStylesHost.removeStyles(this.styles,this.styleUrls)}},Vl=class extends Ma{contentAttr;hostAttr;constructor(e,t,i,r,s,o,a,c){let l=r+"-"+i.id;super(e,t,i,s,o,a,c,l),this.contentAttr=$w(l),this.hostAttr=qw(l)}applyToHost(e){this.applyStyles(),this.setAttribute(e,this.hostAttr,"")}createElement(e,t){let i=super.createElement(e,t);return super.setAttribute(i,this.contentAttr,""),i}};var zl=class n extends va{supportsDOMEvents=!0;static makeCurrent(){Jp(new n)}onAndCancel(e,t,i,r){return e.addEventListener(t,i,r),()=>{e.removeEventListener(t,i,r)}}dispatchEvent(e,t){e.dispatchEvent(t)}remove(e){e.remove()}createElement(e,t){return t=t||this.getDefaultDocument(),t.createElement(e)}createHtmlDocument(){return document.implementation.createHTMLDocument("fakeTitle")}getDefaultDocument(){return document}isElementNode(e){return e.nodeType===Node.ELEMENT_NODE}isShadowRoot(e){return e instanceof DocumentFragment}getGlobalEventTarget(e,t){return t==="window"?window:t==="document"?e:t==="body"?e.body:null}getBaseHref(e){let t=Xw();return t==null?null:Yw(t)}resetBaseElement(){ba=null}getUserAgent(){return window.navigator.userAgent}getCookie(e){return Kp(document.cookie,e)}},ba=null;function Xw(){return ba=ba||document.head.querySelector("base"),ba?ba.getAttribute("href"):null}function Yw(n){return new URL(n,document.baseURI).pathname}var Zw=(()=>{class n{build(){return new XMLHttpRequest}static \u0275fac=function(i){return new(i||n)};static \u0275prov=at({token:n,factory:n.\u0275fac})}return n})(),Nx=["alt","control","meta","shift"],Jw={"\b":"Backspace","	":"Tab","\x7F":"Delete","\x1B":"Escape",Del:"Delete",Esc:"Escape",Left:"ArrowLeft",Right:"ArrowRight",Up:"ArrowUp",Down:"ArrowDown",Menu:"ContextMenu",Scroll:"ScrollLock",Win:"OS"},Kw={alt:n=>n.altKey,control:n=>n.ctrlKey,meta:n=>n.metaKey,shift:n=>n.shiftKey},Px=(()=>{class n extends _a{constructor(t){super(t)}supports(t){return n.parseEventName(t)!=null}addEventListener(t,i,r,s){let o=n.parseEventName(i),a=n.eventCallback(o.fullKey,r,this.manager.getZone());return this.manager.getZone().runOutsideAngular(()=>hs().onAndCancel(t,o.domEventName,a,s))}static parseEventName(t){let i=t.toLowerCase().split("."),r=i.shift();if(i.length===0||!(r==="keydown"||r==="keyup"))return null;let s=n._normalizeKey(i.pop()),o="",a=i.indexOf("code");if(a>-1&&(i.splice(a,1),o="code."),Nx.forEach(l=>{let u=i.indexOf(l);u>-1&&(i.splice(u,1),o+=l+".")}),o+=s,i.length!=0||s.length===0)return null;let c={};return c.domEventName=r,c.fullKey=o,c}static matchEventFullKeyCode(t,i){let r=Jw[t.key]||t.key,s="";return i.indexOf("code.")>-1&&(r=t.code,s="code."),r==null||!r?!1:(r=r.toLowerCase(),r===" "?r="space":r==="."&&(r="dot"),Nx.forEach(o=>{if(o!==r){let a=Kw[o];a(t)&&(s+=o+".")}}),s+=r,s===i)}static eventCallback(t,i,r){return s=>{n.matchEventFullKeyCode(s,t)&&r.runGuarded(()=>i(s))}}static _normalizeKey(t){return t==="esc"?"escape":t}static \u0275fac=function(i){return new(i||n)(rt(on))};static \u0275prov=at({token:n,factory:n.\u0275fac})}return n})();async function om(n,e,t){let i=It({rootComponent:n},Qw(e,t));return Ex(i)}function Qw(n,e){return{platformRef:e?.platformRef,appProviders:[...rC,...n?.providers??[]],platformProviders:iC}}function eC(){zl.makeCurrent()}function tC(){return new Ui}function nC(){return vp(document),document}var iC=[{provide:la,useValue:wx},{provide:bl,useValue:eC,multi:!0},{provide:on,useFactory:nC}];var rC=[{provide:qo,useValue:"root"},{provide:Ui,useFactory:tC},{provide:Hl,useClass:Ul,multi:!0},{provide:Hl,useClass:Px,multi:!0},sm,im,nm,{provide:cs,useExisting:sm},{provide:ya,useClass:Zw},[]];var Lx=(()=>{class n{_doc;_dom;constructor(t){this._doc=t,this._dom=hs()}addTag(t,i=!1){return t?this._getOrCreateElement(t,i):null}addTags(t,i=!1){return t?t.reduce((r,s)=>(s&&r.push(this._getOrCreateElement(s,i)),r),[]):[]}getTag(t){return t&&this._doc.querySelector(`meta[${t}]`)||null}getTags(t){if(!t)return[];let i=this._doc.querySelectorAll(`meta[${t}]`);return i?[].slice.call(i):[]}updateTag(t,i){if(!t)return null;i=i||this._parseSelector(t);let r=this.getTag(i);return r?this._setMetaElementAttributes(t,r):this._getOrCreateElement(t,!0)}removeTag(t){this.removeTagElement(this.getTag(t))}removeTagElement(t){t&&this._dom.remove(t)}_getOrCreateElement(t,i=!1){if(!i){let o=this._parseSelector(t),a=this.getTags(o).filter(c=>this._containsAttributes(t,c))[0];if(a!==void 0)return a}let r=this._dom.createElement("meta");return this._setMetaElementAttributes(t,r),this._doc.getElementsByTagName("head")[0].appendChild(r),r}_setMetaElementAttributes(t,i){return Object.keys(t).forEach(r=>i.setAttribute(this._getMetaKeyMap(r),t[r])),i}_parseSelector(t){let i=t.name?"name":"property";return`${i}="${t[i]}"`}_containsAttributes(t,i){return Object.keys(t).every(r=>i.getAttribute(this._getMetaKeyMap(r))===t[r])}_getMetaKeyMap(t){return sC[t]||t}static \u0275fac=function(i){return new(i||n)(rt(on))};static \u0275prov=at({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})(),sC={httpEquiv:"http-equiv"},Ox=(()=>{class n{_doc;constructor(t){this._doc=t}getTitle(){return this._doc.title}setTitle(t){this._doc.title=t||""}static \u0275fac=function(i){return new(i||n)(rt(on))};static \u0275prov=at({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();var am=(()=>{class n{static \u0275fac=function(i){return new(i||n)};static \u0275prov=at({token:n,factory:function(i){let r=null;return i?r=new(i||n):r=rt(oC),r},providedIn:"root"})}return n})(),oC=(()=>{class n extends am{_doc;constructor(t){super(),this._doc=t}sanitize(t,i){if(i==null)return null;switch(t){case Cn.NONE:return i;case Cn.HTML:return ji(i,"HTML")?Mi(i):Tl(this._doc,String(i)).toString();case Cn.STYLE:return ji(i,"Style")?Mi(i):i;case Cn.SCRIPT:if(ji(i,"Script"))return Mi(i);throw new $e(5200,!1);case Cn.URL:return ji(i,"URL")?Mi(i):ua(String(i));case Cn.RESOURCE_URL:if(ji(i,"ResourceURL"))return Mi(i);throw new $e(5201,!1);default:throw new $e(5202,!1)}}bypassSecurityTrustHtml(t){return _p(t)}bypassSecurityTrustStyle(t){return xp(t)}bypassSecurityTrustScript(t){return Mp(t)}bypassSecurityTrustUrl(t){return bp(t)}bypassSecurityTrustResourceUrl(t){return Sp(t)}static \u0275fac=function(i){return new(i||n)(rt(on))};static \u0275prov=at({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();var Fx={providers:[Mh()]};var kx={languageLabel:"Language",themeLabel:"Theme",nav:[{id:"hero",label:"Home"},{id:"about",label:"About"},{id:"projects",label:"Projects"},{id:"skills",label:"Skills"},{id:"experience",label:"Experience"},{id:"school",label:"Studies"},{id:"contact",label:"Contact"}],hero:{badge:"Corentin PHILYS",title:"Full Stack Developer focused on application development",subtitle:"I design and build robust, performant and maintainable web, mobile, desktop and native applications.",ctaContact:"Contact me",ctaProjects:"View my projects",ctaCv:"Download my resume"},sectionTitles:{about:"About",projects:"Projects",skills:"Skills",experience:"Experience",school:"Studies",contact:"Contact"},about:{title:"Profile",paragraphs:["As a student of application development, I specialize in designing interactive applications and modern interfaces.","I have gained practical experience through the development of applications for connected interactive kiosks, using Angular for web interfaces and C# for various middleware services, as well as through projects in React Native, Java/Kotlin (Android), and Rust.","I am particularly interested in code quality and creating robust solutions integrated into real-world environments.","I am looking for a work-study program that will allow me to strengthen my web and application development skills within a technical team where I can actively contribute to concrete projects.","Curious, self-motivated, and problem-solving oriented, I bring the ability to adapt quickly, an analytical approach, and a genuine motivation to learn and produce reliable, maintainable solutions."]},projects:[{title:"Space Shooter",summary:`Space shooter-style game built using the Godot game engine as part of a university project.
__Objective:__ 3D menu in space with level selection, 2D game with a physics engine. Implementation of various features relevant to a game.
__Achievements:__ Different game modes, integration of a 3D HUD into a 2D game, configuration file for settings, scores, and levels`,stack:["Godot","GdScript","Git"],links:{repository:"#"}},{title:"Aramis",summary:`Mobile app and web-based supervisor interface developed as part of a university project for a company.
__Requirements:__ An app to help order pickers process orders efficiently, along with a web-based administration panel.
__Features:__ Real-time order tracking, 3D pallet visualization, intelligent sorting algorithm, voice recognition.`,stack:["Kotlin","API REST","PostgreSQL","Android SDK","Django","Filament","Vosk","Tests unitaires","Gitlab CI/CD"],links:{}}],skills:[{category:"Frontend",items:["Angular","React","TypeScript","JavaScript","HTML","CSS/SCSS"]},{category:"Backend",items:["Node.js","Express","REST API","SQL","PHP"," PL/pgsql","Django","Firebase"]},{category:"Application",items:["Python","C/C++","C#","Kotlin","Java","Rust"]},{category:"Mobile",items:["Flutter","Kotlin","Java","Android SDK","React Native"]},{category:"Environements",items:["VirtualBox","WSL","Gitlab CI/CD","Docker Compose"]},{category:"Operating System",items:["Linux","Windows","Android"]}],experiences:[{role:"[Apprenticeship] Software development for interactive kiosks",company:"IPM France",period:"09/2025 - 08/2026",details:["Teamwork, formalizing client requirements","Optimizing a complex web application (Angular)","Development of a service for proprietary middleware to support a vital signs monitor (PAX, Vit@Jour) in C#","Tools: Console POC, Gitea server setup, build pipeline and Jenkins plugins"]},{role:"[Internship] Software development for interactive kiosks",company:"IPM France",period:"03/2025 - 06/2025",details:["Teamwork, defining client requirements","Optimizing a complex web application (Angular)","Creating and optimizing demo web applications (Angular)"]}],school:[{diplome:"[Bachelor's degree in science and technology] IT ",school:"IUT de Valence, Valence, Dr\xF4me (26)",period:"09/2023 - 07/2026"},{diplome:"[High school diploma] General (Mathematics, Computer Science, Physics and Chemistry) **Good grade**",school:"Lyc\xE9e \xC9douard Branly, Lyon 05, Rh\xF4ne (69)",period:"09/2021 - 07/2023"}],contact:{title:"Let us discuss your needs",intro:"I am open to apprenticeship opportunities in full stack application development.",emailLabel:"Email",linkedinLabel:"LinkedIn",githubLabel:"GitHub",locationLabel:"Localisation",email:"",linkedin:"https://www.linkedin.com/in/philysc",github:"https://github.com/endercreeps",location:"Lyon, France"},footer:{rights:"\xA9 2026 Corentin PHILYS - All rights reserved."}};var Ux={languageLabel:"Langue",themeLabel:"Theme",nav:[{id:"hero",label:"Accueil"},{id:"about",label:"\xC0 propos"},{id:"projects",label:"Projets"},{id:"skills",label:"Comp\xE9tences"},{id:"experience",label:"Exp\xE9riences"},{id:"school",label:"\xC9tudes"},{id:"contact",label:"Contact"}],hero:{badge:"Corentin PHILYS",title:"D\xE9veloppeur Full Stack orient\xE9 d\xE9veloppement applicatif",subtitle:"Je con\xE7ois et implemente des applications web, mobile, *desktop* et native.",ctaContact:"Me contacter",ctaProjects:"Voir mes projets",ctaCv:"T\xE9l\xE9charger mon CV"},sectionTitles:{about:"\xC0 propos",projects:"Projets",skills:"Comp\xE9tences",experience:"Exp\xE9rience",school:"\xC9tudes",contact:"Contact"},about:{title:"Profil",paragraphs:["\xC9tudiant en d\xE9veloppement applicatif, je me sp\xE9cialise dans la conception d'applications interactives et d'interfaces modernes.","J'ai acquis une exp\xE9rience concr\xE8te \xE0 travers le d\xE9veloppement d'applications pour bornes interactives connect\xE9es, en utilisant notamment Angular pour les interfaces web ainsi que C# pour les diff\xE9rents services d'un *middleware* mais aussi via des projets en React Native, Java/Kotlin (Android) et Rust.","Je m'int\xE9resse particuli\xE8rement \xE0 la qualit\xE9 du code et \xE0 la cr\xE9ation de solutions robustes int\xE9gr\xE9es dans des environnements r\xE9els.","Je recherche une alternance me permettant de renforcer mes comp\xE9tences en d\xE9veloppement web et applicatif au sein d'une \xE9quipe technique o\xF9 je pourrai contribuer activement \xE0 des projets concrets.","Curieux, autonome et orient\xE9 r\xE9solution de probl\xE8mes, j'apporte une capacit\xE9 d'adaptation rapide, une approche analytique et une r\xE9elle motivation \xE0 apprendre et \xE0 produire des solutions fiables et maintenables."]},projects:[{title:"Space Shooter",summary:`Jeu type *Space Shooter* avec le moteur de jeu Godot dans le cadre d'un projet universitaire.
__Objectif :__ Menu 3D dans l'espace avec choix de niveaux, jeu en 2D avec moteur physique. Impl\xE9mentation de diff\xE9rentes fonctionnalit\xE9s pertinente pour un jeu.
__R\xE9alisation :__ Diff\xE9rents mode de jeu, int\xE9gration d'un *HUD* 3D dans un jeu en 2D, fichier de configuration pour les param\xE8tres, les scores et les niveaux`,stack:["Godot","GdScript","Git"],links:{repository:"#"}},{title:"Aramis",summary:`Application mobile et interface superviseur web dans le cadre d'un projet universitaire pour une entreprise.
__Besoin :__ application pour aider les pr\xE9parateurs \xE0 r\xE9aliser les commandes de mani\xE8re efficace et administration sur un pannel web.
__R\xE9alisation :__ Suivi des pr\xE9parations en temps r\xE9el, visualisation de palette en 3D, algorithme de tri intelligent, reconnaisance vocal.`,stack:["Kotlin","API REST","PostgreSQL","Android SDK","Django","Filament","Vosk","Tests unitaires","Gitlab CI/CD"],links:{}}],skills:[{category:"Frontend",items:["Angular","React","TypeScript","JavaScript","HTML","CSS/SCSS"]},{category:"Backend",items:["Node.js","Express","REST API","SQL","PHP"," PL/pgsql","Django","Firebase"]},{category:"Applicatif",items:["Python","C/C++","C#","Kotlin","Java","Rust"]},{category:"Mobile",items:["Flutter","Kotlin","Java","Android SDK","React Native"]},{category:"Environnements",items:["VirtualBox","WSL","Gitlab CI/CD","Docker Compose"]},{category:"Syst\xE8mes d'exploitation",items:["Linux","Windows","Android"]}],experiences:[{role:"[Alternance] D\xE9veloppement logiciel de bornes interactives",company:"IPM France",period:"09/2025 - 08/2026",details:["Travail d'\xE9quipe, formalisation du besoin client","Optimisation d'une application web complexe (Angular)","R\xE9alisation d'un service pour middleware propri\xE9taire pour l'utilisation d'un lecteur vitale (PAX, Vit@Jour) en C#.","Outillage : POC console, mise en place serveur Gitea, chaine de compilation et plugins Jenkins"]},{role:"[Stage] D\xE9veloppement logiciel de bornes interactives",company:"IPM France",period:"03/2025 - 06/2025",details:["Travail d'\xE9quipe, formalisation du besoin client","Optimisation d'une application web complexe (Angular)","Cr\xE9ation et optimisations d'aplications web de d\xE9monstration (Angular)"]}],school:[{diplome:"[BUT] Informatique (Parcours r\xE9alisation et d\xE9veloppement d'application)",school:"IUT de Valence, Valence, Dr\xF4me (26)",period:"09/2023 - 07/2026"},{diplome:"[BAC] G\xE9n\xE9ral (Math\xE9matiques, Num\xE9rique et Science Informatiques, Physique-Chimie) **Mention Bien**",school:"Lyc\xE9e \xC9douard Branly, Lyon 05, Rh\xF4ne (69)",period:"09/2021 - 07/2023"}],contact:{title:"\xC9changeons sur votre besoin",intro:"Je suis ouvert aux opportunit\xE9s d'alternance en d\xE9veloppement applicatif.",emailLabel:"Email",linkedinLabel:"LinkedIn",githubLabel:"GitHub",locationLabel:"Localisation",email:"",linkedin:"https://www.linkedin.com/in/philysc",github:"https://github.com/endercreeps",location:"Lyon, France"},footer:{rights:"\xA9 2026 Corentin PHILYS - Tous droits r\xE9serv\xE9s."}};var ao={name:"Portfolio",defaultLanguage:"fr",cvPath:"/cv/cv.pdf",seo:{fr:{title:"Portfolio | Developpeur Full Stack",description:"Portfolio one-page professionnel orient\xE9 developpement applicatif full stack, disponible en francais et anglais."},en:{title:"Portfolio | Full Stack Developer",description:"Professional one-page portfolio focused on full stack application development, available in French and English."}}};var Bx="portfolio-language",Dt=class n{currentLanguage=an(this.resolveInitialLanguage());language=xr(()=>this.currentLanguage());content=xr(()=>this.currentLanguage()==="fr"?Ux:kx);setLanguage(e){this.currentLanguage.set(e),localStorage.setItem(Bx,e),document.documentElement.lang=e}toggleLanguage(){this.setLanguage(this.currentLanguage()==="fr"?"en":"fr")}resolveInitialLanguage(){let e=localStorage.getItem(Bx),t=e==="fr"||e==="en"?e:ao.defaultLanguage;return document.documentElement.lang=t,t}static \u0275fac=function(t){return new(t||n)};static \u0275prov=at({token:n,factory:n.\u0275fac,providedIn:"root"})};var Vx="portfolio-theme",Gl=class n{currentTheme=an(this.resolveInitialTheme());theme=xr(()=>this.currentTheme());constructor(){this.applyTheme(this.currentTheme())}toggleTheme(){let e=this.currentTheme()==="dark"?"light":"dark";this.setTheme(e)}setTheme(e){this.currentTheme.set(e),this.applyTheme(e),localStorage.setItem(Vx,e)}applyTheme(e){document.documentElement.setAttribute("data-theme",e)}resolveInitialTheme(){let e=localStorage.getItem(Vx);return e==="light"||e==="dark"?e:typeof window<"u"&&typeof window.matchMedia=="function"&&window.matchMedia("(prefers-color-scheme: dark)").matches?"dark":"light"}static \u0275fac=function(t){return new(t||n)};static \u0275prov=at({token:n,factory:n.\u0275fac,providedIn:"root"})};var Rt=class n{sanitizer=me(am);transform(e){if(!e)return"";let i=this.escapeHtml(e).replace(/\*\*(.+?)\*\*/g,"<strong>$1</strong>").replace(/__(.+?)__/g,"<u>$1</u>").replace(/(?<!\*)\*(?!\s)(.+?)(?<!\s)\*(?!\*)/g,"<em>$1</em>").replace(/\n/g,"<br>");return this.sanitizer.sanitize(Cn.HTML,i)??""}escapeHtml(e){return e.replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;").replace(/"/g,"&quot;").replace(/'/g,"&#39;")}static \u0275fac=function(t){return new(t||n)};static \u0275pipe=zp({name:"richText",type:n,pure:!0})};var aC=(n,e)=>e.id;function cC(n,e){if(n&1){let t=ro();kt(0,"button",14),Qe(1,"richText"),_r("click",function(){let r=gr(t).$implicit,s=Gn();return vr(s.scrollToSection(r.id))}),$t()}if(n&2){let t=e.$implicit;en("innerHTML",et(1,1,t.label),Je)}}var Wl=class n{i18n=me(Dt);theme=me(Gl);content=this.i18n.content;language=this.i18n.language;currentTheme=this.theme.theme;scrollToSection(e){document.getElementById(e)?.scrollIntoView({behavior:"smooth",block:"start"})}switchLanguage(e){this.i18n.setLanguage(e)}toggleTheme(){this.theme.toggleTheme()}static \u0275fac=function(t){return new(t||n)};static \u0275cmp=Et({type:n,selectors:[["app-header"]],decls:20,vars:9,consts:[["role","banner",1,"site-header"],[1,"header-inner","container"],["type","button","aria-label","Home section",1,"brand",3,"click"],["src","logo.png","alt","Portfolio",1,"brand-logo"],["aria-label","Primary",1,"nav"],["type","button",1,"nav-link",3,"innerHTML"],[1,"controls"],[1,"language-switch"],["type","button","aria-label","Switch to French",3,"click"],["type","button","aria-label","Switch to English",3,"click"],["type","button",1,"theme-toggle",3,"click"],["aria-hidden","true",1,"theme-glyph"],["aria-hidden","true",1,"theme-switch-track"],[1,"theme-switch-thumb"],["type","button",1,"nav-link",3,"click","innerHTML"]],template:function(t,i){t&1&&(kt(0,"header",0)(1,"div",1)(2,"button",2),_r("click",function(){return i.scrollToSection("hero")}),Vt(3,"img",3),$t(),kt(4,"nav",4),cn(5,cC,2,3,"button",5,aC),$t(),kt(7,"div",6)(8,"div",7)(9,"button",8),_r("click",function(){return i.switchLanguage("fr")}),bt(10," FR "),$t(),kt(11,"button",9),_r("click",function(){return i.switchLanguage("en")}),bt(12," EN "),$t()(),kt(13,"button",10),_r("click",function(){return i.toggleTheme()}),kt(14,"span",11),bt(15,"\u2600"),$t(),kt(16,"span",12),Vt(17,"span",13),$t(),kt(18,"span",11),bt(19,"\u263E"),$t()()()()()),t&2&&(le(5),ln(i.content().nav),le(3),ds("aria-label",i.content().languageLabel),le(),oo("is-active",i.language()==="fr"),le(2),oo("is-active",i.language()==="en"),le(2),oo("is-dark",i.currentTheme()==="dark"),ds("aria-label",i.content().themeLabel)("aria-pressed",i.currentTheme()==="dark"))},dependencies:[Rt],styles:[".site-header[_ngcontent-%COMP%]{position:sticky;top:0;z-index:40;background:color-mix(in srgb,var(--surface-primary) 84%,transparent);-webkit-backdrop-filter:blur(12px);backdrop-filter:blur(12px);border-bottom:1px solid var(--border-color)}.header-inner[_ngcontent-%COMP%]{min-height:4.5rem;display:flex;align-items:center;justify-content:space-between;gap:1rem}.brand[_ngcontent-%COMP%]{border:none;background:transparent;padding:.2rem;line-height:0;cursor:pointer}.brand-logo[_ngcontent-%COMP%]{display:block;height:2.5rem;width:auto}[data-theme=dark][_nghost-%COMP%]   .brand-logo[_ngcontent-%COMP%], [data-theme=dark]   [_nghost-%COMP%]   .brand-logo[_ngcontent-%COMP%]{filter:invert(1) brightness(1.35) contrast(1.08)}.nav[_ngcontent-%COMP%]{display:none;gap:.35rem}.nav-link[_ngcontent-%COMP%], .language-switch[_ngcontent-%COMP%]   button[_ngcontent-%COMP%]{border:1px solid transparent;background:transparent;color:var(--text-secondary);border-radius:.5rem;padding:.45rem .65rem;cursor:pointer;transition:color .18s ease,background-color .18s ease,border-color .18s ease}.nav-link[_ngcontent-%COMP%]:hover, .language-switch[_ngcontent-%COMP%]   button[_ngcontent-%COMP%]:hover{color:var(--text-primary);border-color:var(--border-color);background:var(--surface-secondary)}.theme-toggle[_ngcontent-%COMP%]{border:1px solid var(--border-color);background:linear-gradient(90deg,color-mix(in srgb,#ffd857 26%,var(--surface-secondary)),color-mix(in srgb,#1e3a8a 26%,var(--surface-secondary)));color:var(--text-secondary);border-radius:999px;padding:.28rem .45rem;cursor:pointer;display:inline-flex;align-items:center;gap:.35rem;transition:border-color .18s ease,background-color .18s ease}.theme-toggle[_ngcontent-%COMP%]:hover{border-color:color-mix(in srgb,var(--accent) 35%,var(--border-color));background:var(--surface-secondary)}.theme-glyph[_ngcontent-%COMP%]{font-size:.75rem;line-height:1}.theme-glyph[_ngcontent-%COMP%]:first-child{color:#f4b400;text-shadow:0 0 8px rgba(255,216,87,.45)}.theme-glyph[_ngcontent-%COMP%]:last-child{color:#1e3a8a;text-shadow:0 0 8px rgba(30,58,138,.35)}.theme-switch-track[_ngcontent-%COMP%]{position:relative;width:2.25rem;height:1.2rem;border-radius:999px;background:linear-gradient(90deg,color-mix(in srgb,#ffd857 35%,var(--surface-secondary)),color-mix(in srgb,#1e3a8a 40%,var(--surface-secondary)));border:1px solid color-mix(in srgb,#1e3a8a 20%,var(--border-color))}.theme-switch-thumb[_ngcontent-%COMP%]{position:absolute;top:50%;left:.14rem;width:.82rem;height:.82rem;border-radius:50%;background:color-mix(in srgb,var(--surface-primary) 92%,white 8%);transform:translateY(-50%);box-shadow:0 2px 6px #00000047;transition:left .18s ease,background-color .18s ease}.theme-toggle.is-dark[_ngcontent-%COMP%]   .theme-switch-thumb[_ngcontent-%COMP%]{left:1.18rem;background:color-mix(in srgb,var(--accent) 22%,white 78%)}.theme-toggle.is-dark[_ngcontent-%COMP%]   .theme-switch-track[_ngcontent-%COMP%]{background:linear-gradient(90deg,color-mix(in srgb,#ffd857 22%,var(--surface-secondary)),color-mix(in srgb,#1e3a8a 55%,var(--surface-secondary)))}.controls[_ngcontent-%COMP%]{display:flex;align-items:center;gap:.5rem}.language-switch[_ngcontent-%COMP%]{display:inline-flex;gap:.2rem;padding:.22rem;border:1px solid var(--border-color);border-radius:999px;background:color-mix(in srgb,var(--surface-primary) 88%,transparent)}.language-switch[_ngcontent-%COMP%]   button[_ngcontent-%COMP%]{min-width:2.2rem;padding:.22rem .6rem;border-radius:999px;font-size:.76rem;font-weight:700;letter-spacing:.05em;text-transform:uppercase}.language-switch[_ngcontent-%COMP%]   .is-active[_ngcontent-%COMP%]{background:color-mix(in srgb,var(--accent) 20%,var(--surface-primary));color:var(--text-primary);border-color:color-mix(in srgb,var(--accent) 28%,var(--border-color));box-shadow:inset 0 0 0 1px #ffffff2e}@media(min-width:920px){.nav[_ngcontent-%COMP%]{display:inline-flex}}"]})};var o0=0,Vm=1,a0=2;var Ya=1,c0=2,Co=3,er=0,vn=1,Ii=2,Ai=0,tr=1,Hm=2,zm=3,Gm=4,l0=5;var Ir=100,u0=101,d0=102,f0=103,h0=104,p0=200,m0=201,g0=202,v0=203,pu=204,mu=205,y0=206,_0=207,x0=208,M0=209,b0=210,S0=211,E0=212,T0=213,w0=214,gu=0,vu=1,yu=2,xs=3,_u=4,xu=5,Mu=6,bu=7,Wm=0,C0=1,I0=2,di=0,jm=1,$m=2,qm=3,Xm=4,Ym=5,Zm=6,Jm=7;var Rm=300,Lr=301,Es=302,$u=303,qu=304,Za=306,Su=1e3,Ei=1001,Eu=1002,Kt=1003,A0=1004;var Ja=1005;var tn=1006,Xu=1007;var Or=1008;var On=1009,Km=1010,Qm=1011,Io=1012,Yu=1013,fi=1014,hi=1015,Di=1016,Zu=1017,Ju=1018,Ao=1020,eg=35902,tg=35899,ng=1021,ig=1022,Yn=1023,Ti=1026,Fr=1027,rg=1028,Ku=1029,Ts=1030,Qu=1031;var ed=1033,Ka=33776,Qa=33777,ec=33778,tc=33779,td=35840,nd=35841,id=35842,rd=35843,sd=36196,od=37492,ad=37496,cd=37488,ld=37489,ud=37490,dd=37491,fd=37808,hd=37809,pd=37810,md=37811,gd=37812,vd=37813,yd=37814,_d=37815,xd=37816,Md=37817,bd=37818,Sd=37819,Ed=37820,Td=37821,wd=36492,Cd=36494,Id=36495,Ad=36283,Dd=36284,Rd=36285,Nd=36286;var Ia=2300,Tu=2301,hu=2302,Nm=2303,Pm=2400,Lm=2401,Om=2402;var D0=3200;var R0=0,N0=1,ir="",Dn="srgb",Ms="srgb-linear",Aa="linear",ut="srgb";var ys=7680;var Fm=519,P0=512,L0=513,O0=514,Pd=515,F0=516,k0=517,Ld=518,U0=519,km=35044;var sg="300 es",ui=2e3,Da=2001;function lC(n){for(let e=n.length-1;e>=0;--e)if(n[e]>=65535)return!0;return!1}function uC(n){return ArrayBuffer.isView(n)&&!(n instanceof DataView)}function Ra(n){return document.createElementNS("http://www.w3.org/1999/xhtml",n)}function B0(){let n=Ra("canvas");return n.style.display="block",n}var Hx={},bo=null;function og(...n){let e="THREE."+n.shift();bo?bo("log",e,...n):console.log(e,...n)}function V0(n){let e=n[0];if(typeof e=="string"&&e.startsWith("TSL:")){let t=n[1];t&&t.isStackTrace?n[0]+=" "+t.getLocation():n[1]='Stack trace not available. Enable "THREE.Node.captureStackTrace" to capture stack traces.'}return n}function Ne(...n){n=V0(n);let e="THREE."+n.shift();if(bo)bo("warn",e,...n);else{let t=n[0];t&&t.isStackTrace?console.warn(t.getError(e)):console.warn(e,...n)}}function Re(...n){n=V0(n);let e="THREE."+n.shift();if(bo)bo("error",e,...n);else{let t=n[0];t&&t.isStackTrace?console.error(t.getError(e)):console.error(e,...n)}}function Na(...n){let e=n.join(" ");e in Hx||(Hx[e]=!0,Ne(...n))}function H0(n,e,t){return new Promise(function(i,r){function s(){switch(n.clientWaitSync(e,n.SYNC_FLUSH_COMMANDS_BIT,0)){case n.WAIT_FAILED:r();break;case n.TIMEOUT_EXPIRED:setTimeout(s,t);break;default:i()}}setTimeout(s,t)})}var z0={[gu]:vu,[yu]:Mu,[_u]:bu,[xs]:xu,[vu]:gu,[Mu]:yu,[bu]:_u,[xu]:xs},nr=class{addEventListener(e,t){this._listeners===void 0&&(this._listeners={});let i=this._listeners;i[e]===void 0&&(i[e]=[]),i[e].indexOf(t)===-1&&i[e].push(t)}hasEventListener(e,t){let i=this._listeners;return i===void 0?!1:i[e]!==void 0&&i[e].indexOf(t)!==-1}removeEventListener(e,t){let i=this._listeners;if(i===void 0)return;let r=i[e];if(r!==void 0){let s=r.indexOf(t);s!==-1&&r.splice(s,1)}}dispatchEvent(e){let t=this._listeners;if(t===void 0)return;let i=t[e.type];if(i!==void 0){e.target=this;let r=i.slice(0);for(let s=0,o=r.length;s<o;s++)r[s].call(this,e);e.target=null}}},un=["00","01","02","03","04","05","06","07","08","09","0a","0b","0c","0d","0e","0f","10","11","12","13","14","15","16","17","18","19","1a","1b","1c","1d","1e","1f","20","21","22","23","24","25","26","27","28","29","2a","2b","2c","2d","2e","2f","30","31","32","33","34","35","36","37","38","39","3a","3b","3c","3d","3e","3f","40","41","42","43","44","45","46","47","48","49","4a","4b","4c","4d","4e","4f","50","51","52","53","54","55","56","57","58","59","5a","5b","5c","5d","5e","5f","60","61","62","63","64","65","66","67","68","69","6a","6b","6c","6d","6e","6f","70","71","72","73","74","75","76","77","78","79","7a","7b","7c","7d","7e","7f","80","81","82","83","84","85","86","87","88","89","8a","8b","8c","8d","8e","8f","90","91","92","93","94","95","96","97","98","99","9a","9b","9c","9d","9e","9f","a0","a1","a2","a3","a4","a5","a6","a7","a8","a9","aa","ab","ac","ad","ae","af","b0","b1","b2","b3","b4","b5","b6","b7","b8","b9","ba","bb","bc","bd","be","bf","c0","c1","c2","c3","c4","c5","c6","c7","c8","c9","ca","cb","cc","cd","ce","cf","d0","d1","d2","d3","d4","d5","d6","d7","d8","d9","da","db","dc","dd","de","df","e0","e1","e2","e3","e4","e5","e6","e7","e8","e9","ea","eb","ec","ed","ee","ef","f0","f1","f2","f3","f4","f5","f6","f7","f8","f9","fa","fb","fc","fd","fe","ff"];var cm=Math.PI/180,wu=180/Math.PI;function nc(){let n=Math.random()*4294967295|0,e=Math.random()*4294967295|0,t=Math.random()*4294967295|0,i=Math.random()*4294967295|0;return(un[n&255]+un[n>>8&255]+un[n>>16&255]+un[n>>24&255]+"-"+un[e&255]+un[e>>8&255]+"-"+un[e>>16&15|64]+un[e>>24&255]+"-"+un[t&63|128]+un[t>>8&255]+"-"+un[t>>16&255]+un[t>>24&255]+un[i&255]+un[i>>8&255]+un[i>>16&255]+un[i>>24&255]).toLowerCase()}function Ze(n,e,t){return Math.max(e,Math.min(t,n))}function dC(n,e){return(n%e+e)%e}function lm(n,e,t){return(1-t)*n+t*e}function Sa(n,e){switch(e.constructor){case Float32Array:return n;case Uint32Array:return n/4294967295;case Uint16Array:return n/65535;case Uint8Array:return n/255;case Int32Array:return Math.max(n/2147483647,-1);case Int16Array:return Math.max(n/32767,-1);case Int8Array:return Math.max(n/127,-1);default:throw new Error("Invalid component type.")}}function xn(n,e){switch(e.constructor){case Float32Array:return n;case Uint32Array:return Math.round(n*4294967295);case Uint16Array:return Math.round(n*65535);case Uint8Array:return Math.round(n*255);case Int32Array:return Math.round(n*2147483647);case Int16Array:return Math.round(n*32767);case Int8Array:return Math.round(n*127);default:throw new Error("Invalid component type.")}}var ht=class n{constructor(e=0,t=0){n.prototype.isVector2=!0,this.x=e,this.y=t}get width(){return this.x}set width(e){this.x=e}get height(){return this.y}set height(e){this.y=e}set(e,t){return this.x=e,this.y=t,this}setScalar(e){return this.x=e,this.y=e,this}setX(e){return this.x=e,this}setY(e){return this.y=e,this}setComponent(e,t){switch(e){case 0:this.x=t;break;case 1:this.y=t;break;default:throw new Error("index is out of range: "+e)}return this}getComponent(e){switch(e){case 0:return this.x;case 1:return this.y;default:throw new Error("index is out of range: "+e)}}clone(){return new this.constructor(this.x,this.y)}copy(e){return this.x=e.x,this.y=e.y,this}add(e){return this.x+=e.x,this.y+=e.y,this}addScalar(e){return this.x+=e,this.y+=e,this}addVectors(e,t){return this.x=e.x+t.x,this.y=e.y+t.y,this}addScaledVector(e,t){return this.x+=e.x*t,this.y+=e.y*t,this}sub(e){return this.x-=e.x,this.y-=e.y,this}subScalar(e){return this.x-=e,this.y-=e,this}subVectors(e,t){return this.x=e.x-t.x,this.y=e.y-t.y,this}multiply(e){return this.x*=e.x,this.y*=e.y,this}multiplyScalar(e){return this.x*=e,this.y*=e,this}divide(e){return this.x/=e.x,this.y/=e.y,this}divideScalar(e){return this.multiplyScalar(1/e)}applyMatrix3(e){let t=this.x,i=this.y,r=e.elements;return this.x=r[0]*t+r[3]*i+r[6],this.y=r[1]*t+r[4]*i+r[7],this}min(e){return this.x=Math.min(this.x,e.x),this.y=Math.min(this.y,e.y),this}max(e){return this.x=Math.max(this.x,e.x),this.y=Math.max(this.y,e.y),this}clamp(e,t){return this.x=Ze(this.x,e.x,t.x),this.y=Ze(this.y,e.y,t.y),this}clampScalar(e,t){return this.x=Ze(this.x,e,t),this.y=Ze(this.y,e,t),this}clampLength(e,t){let i=this.length();return this.divideScalar(i||1).multiplyScalar(Ze(i,e,t))}floor(){return this.x=Math.floor(this.x),this.y=Math.floor(this.y),this}ceil(){return this.x=Math.ceil(this.x),this.y=Math.ceil(this.y),this}round(){return this.x=Math.round(this.x),this.y=Math.round(this.y),this}roundToZero(){return this.x=Math.trunc(this.x),this.y=Math.trunc(this.y),this}negate(){return this.x=-this.x,this.y=-this.y,this}dot(e){return this.x*e.x+this.y*e.y}cross(e){return this.x*e.y-this.y*e.x}lengthSq(){return this.x*this.x+this.y*this.y}length(){return Math.sqrt(this.x*this.x+this.y*this.y)}manhattanLength(){return Math.abs(this.x)+Math.abs(this.y)}normalize(){return this.divideScalar(this.length()||1)}angle(){return Math.atan2(-this.y,-this.x)+Math.PI}angleTo(e){let t=Math.sqrt(this.lengthSq()*e.lengthSq());if(t===0)return Math.PI/2;let i=this.dot(e)/t;return Math.acos(Ze(i,-1,1))}distanceTo(e){return Math.sqrt(this.distanceToSquared(e))}distanceToSquared(e){let t=this.x-e.x,i=this.y-e.y;return t*t+i*i}manhattanDistanceTo(e){return Math.abs(this.x-e.x)+Math.abs(this.y-e.y)}setLength(e){return this.normalize().multiplyScalar(e)}lerp(e,t){return this.x+=(e.x-this.x)*t,this.y+=(e.y-this.y)*t,this}lerpVectors(e,t,i){return this.x=e.x+(t.x-e.x)*i,this.y=e.y+(t.y-e.y)*i,this}equals(e){return e.x===this.x&&e.y===this.y}fromArray(e,t=0){return this.x=e[t],this.y=e[t+1],this}toArray(e=[],t=0){return e[t]=this.x,e[t+1]=this.y,e}fromBufferAttribute(e,t){return this.x=e.getX(t),this.y=e.getY(t),this}rotateAround(e,t){let i=Math.cos(t),r=Math.sin(t),s=this.x-e.x,o=this.y-e.y;return this.x=s*i-o*r+e.x,this.y=s*r+o*i+e.y,this}random(){return this.x=Math.random(),this.y=Math.random(),this}*[Symbol.iterator](){yield this.x,yield this.y}},wi=class{constructor(e=0,t=0,i=0,r=1){this.isQuaternion=!0,this._x=e,this._y=t,this._z=i,this._w=r}static slerpFlat(e,t,i,r,s,o,a){let c=i[r+0],l=i[r+1],u=i[r+2],f=i[r+3],d=s[o+0],h=s[o+1],g=s[o+2],x=s[o+3];if(f!==x||c!==d||l!==h||u!==g){let m=c*d+l*h+u*g+f*x;m<0&&(d=-d,h=-h,g=-g,x=-x,m=-m);let p=1-a;if(m<.9995){let M=Math.acos(m),T=Math.sin(M);p=Math.sin(p*M)/T,a=Math.sin(a*M)/T,c=c*p+d*a,l=l*p+h*a,u=u*p+g*a,f=f*p+x*a}else{c=c*p+d*a,l=l*p+h*a,u=u*p+g*a,f=f*p+x*a;let M=1/Math.sqrt(c*c+l*l+u*u+f*f);c*=M,l*=M,u*=M,f*=M}}e[t]=c,e[t+1]=l,e[t+2]=u,e[t+3]=f}static multiplyQuaternionsFlat(e,t,i,r,s,o){let a=i[r],c=i[r+1],l=i[r+2],u=i[r+3],f=s[o],d=s[o+1],h=s[o+2],g=s[o+3];return e[t]=a*g+u*f+c*h-l*d,e[t+1]=c*g+u*d+l*f-a*h,e[t+2]=l*g+u*h+a*d-c*f,e[t+3]=u*g-a*f-c*d-l*h,e}get x(){return this._x}set x(e){this._x=e,this._onChangeCallback()}get y(){return this._y}set y(e){this._y=e,this._onChangeCallback()}get z(){return this._z}set z(e){this._z=e,this._onChangeCallback()}get w(){return this._w}set w(e){this._w=e,this._onChangeCallback()}set(e,t,i,r){return this._x=e,this._y=t,this._z=i,this._w=r,this._onChangeCallback(),this}clone(){return new this.constructor(this._x,this._y,this._z,this._w)}copy(e){return this._x=e.x,this._y=e.y,this._z=e.z,this._w=e.w,this._onChangeCallback(),this}setFromEuler(e,t=!0){let i=e._x,r=e._y,s=e._z,o=e._order,a=Math.cos,c=Math.sin,l=a(i/2),u=a(r/2),f=a(s/2),d=c(i/2),h=c(r/2),g=c(s/2);switch(o){case"XYZ":this._x=d*u*f+l*h*g,this._y=l*h*f-d*u*g,this._z=l*u*g+d*h*f,this._w=l*u*f-d*h*g;break;case"YXZ":this._x=d*u*f+l*h*g,this._y=l*h*f-d*u*g,this._z=l*u*g-d*h*f,this._w=l*u*f+d*h*g;break;case"ZXY":this._x=d*u*f-l*h*g,this._y=l*h*f+d*u*g,this._z=l*u*g+d*h*f,this._w=l*u*f-d*h*g;break;case"ZYX":this._x=d*u*f-l*h*g,this._y=l*h*f+d*u*g,this._z=l*u*g-d*h*f,this._w=l*u*f+d*h*g;break;case"YZX":this._x=d*u*f+l*h*g,this._y=l*h*f+d*u*g,this._z=l*u*g-d*h*f,this._w=l*u*f-d*h*g;break;case"XZY":this._x=d*u*f-l*h*g,this._y=l*h*f-d*u*g,this._z=l*u*g+d*h*f,this._w=l*u*f+d*h*g;break;default:Ne("Quaternion: .setFromEuler() encountered an unknown order: "+o)}return t===!0&&this._onChangeCallback(),this}setFromAxisAngle(e,t){let i=t/2,r=Math.sin(i);return this._x=e.x*r,this._y=e.y*r,this._z=e.z*r,this._w=Math.cos(i),this._onChangeCallback(),this}setFromRotationMatrix(e){let t=e.elements,i=t[0],r=t[4],s=t[8],o=t[1],a=t[5],c=t[9],l=t[2],u=t[6],f=t[10],d=i+a+f;if(d>0){let h=.5/Math.sqrt(d+1);this._w=.25/h,this._x=(u-c)*h,this._y=(s-l)*h,this._z=(o-r)*h}else if(i>a&&i>f){let h=2*Math.sqrt(1+i-a-f);this._w=(u-c)/h,this._x=.25*h,this._y=(r+o)/h,this._z=(s+l)/h}else if(a>f){let h=2*Math.sqrt(1+a-i-f);this._w=(s-l)/h,this._x=(r+o)/h,this._y=.25*h,this._z=(c+u)/h}else{let h=2*Math.sqrt(1+f-i-a);this._w=(o-r)/h,this._x=(s+l)/h,this._y=(c+u)/h,this._z=.25*h}return this._onChangeCallback(),this}setFromUnitVectors(e,t){let i=e.dot(t)+1;return i<1e-8?(i=0,Math.abs(e.x)>Math.abs(e.z)?(this._x=-e.y,this._y=e.x,this._z=0,this._w=i):(this._x=0,this._y=-e.z,this._z=e.y,this._w=i)):(this._x=e.y*t.z-e.z*t.y,this._y=e.z*t.x-e.x*t.z,this._z=e.x*t.y-e.y*t.x,this._w=i),this.normalize()}angleTo(e){return 2*Math.acos(Math.abs(Ze(this.dot(e),-1,1)))}rotateTowards(e,t){let i=this.angleTo(e);if(i===0)return this;let r=Math.min(1,t/i);return this.slerp(e,r),this}identity(){return this.set(0,0,0,1)}invert(){return this.conjugate()}conjugate(){return this._x*=-1,this._y*=-1,this._z*=-1,this._onChangeCallback(),this}dot(e){return this._x*e._x+this._y*e._y+this._z*e._z+this._w*e._w}lengthSq(){return this._x*this._x+this._y*this._y+this._z*this._z+this._w*this._w}length(){return Math.sqrt(this._x*this._x+this._y*this._y+this._z*this._z+this._w*this._w)}normalize(){let e=this.length();return e===0?(this._x=0,this._y=0,this._z=0,this._w=1):(e=1/e,this._x=this._x*e,this._y=this._y*e,this._z=this._z*e,this._w=this._w*e),this._onChangeCallback(),this}multiply(e){return this.multiplyQuaternions(this,e)}premultiply(e){return this.multiplyQuaternions(e,this)}multiplyQuaternions(e,t){let i=e._x,r=e._y,s=e._z,o=e._w,a=t._x,c=t._y,l=t._z,u=t._w;return this._x=i*u+o*a+r*l-s*c,this._y=r*u+o*c+s*a-i*l,this._z=s*u+o*l+i*c-r*a,this._w=o*u-i*a-r*c-s*l,this._onChangeCallback(),this}slerp(e,t){let i=e._x,r=e._y,s=e._z,o=e._w,a=this.dot(e);a<0&&(i=-i,r=-r,s=-s,o=-o,a=-a);let c=1-t;if(a<.9995){let l=Math.acos(a),u=Math.sin(l);c=Math.sin(c*l)/u,t=Math.sin(t*l)/u,this._x=this._x*c+i*t,this._y=this._y*c+r*t,this._z=this._z*c+s*t,this._w=this._w*c+o*t,this._onChangeCallback()}else this._x=this._x*c+i*t,this._y=this._y*c+r*t,this._z=this._z*c+s*t,this._w=this._w*c+o*t,this.normalize();return this}slerpQuaternions(e,t,i){return this.copy(e).slerp(t,i)}random(){let e=2*Math.PI*Math.random(),t=2*Math.PI*Math.random(),i=Math.random(),r=Math.sqrt(1-i),s=Math.sqrt(i);return this.set(r*Math.sin(e),r*Math.cos(e),s*Math.sin(t),s*Math.cos(t))}equals(e){return e._x===this._x&&e._y===this._y&&e._z===this._z&&e._w===this._w}fromArray(e,t=0){return this._x=e[t],this._y=e[t+1],this._z=e[t+2],this._w=e[t+3],this._onChangeCallback(),this}toArray(e=[],t=0){return e[t]=this._x,e[t+1]=this._y,e[t+2]=this._z,e[t+3]=this._w,e}fromBufferAttribute(e,t){return this._x=e.getX(t),this._y=e.getY(t),this._z=e.getZ(t),this._w=e.getW(t),this._onChangeCallback(),this}toJSON(){return this.toArray()}_onChange(e){return this._onChangeCallback=e,this}_onChangeCallback(){}*[Symbol.iterator](){yield this._x,yield this._y,yield this._z,yield this._w}},k=class n{constructor(e=0,t=0,i=0){n.prototype.isVector3=!0,this.x=e,this.y=t,this.z=i}set(e,t,i){return i===void 0&&(i=this.z),this.x=e,this.y=t,this.z=i,this}setScalar(e){return this.x=e,this.y=e,this.z=e,this}setX(e){return this.x=e,this}setY(e){return this.y=e,this}setZ(e){return this.z=e,this}setComponent(e,t){switch(e){case 0:this.x=t;break;case 1:this.y=t;break;case 2:this.z=t;break;default:throw new Error("index is out of range: "+e)}return this}getComponent(e){switch(e){case 0:return this.x;case 1:return this.y;case 2:return this.z;default:throw new Error("index is out of range: "+e)}}clone(){return new this.constructor(this.x,this.y,this.z)}copy(e){return this.x=e.x,this.y=e.y,this.z=e.z,this}add(e){return this.x+=e.x,this.y+=e.y,this.z+=e.z,this}addScalar(e){return this.x+=e,this.y+=e,this.z+=e,this}addVectors(e,t){return this.x=e.x+t.x,this.y=e.y+t.y,this.z=e.z+t.z,this}addScaledVector(e,t){return this.x+=e.x*t,this.y+=e.y*t,this.z+=e.z*t,this}sub(e){return this.x-=e.x,this.y-=e.y,this.z-=e.z,this}subScalar(e){return this.x-=e,this.y-=e,this.z-=e,this}subVectors(e,t){return this.x=e.x-t.x,this.y=e.y-t.y,this.z=e.z-t.z,this}multiply(e){return this.x*=e.x,this.y*=e.y,this.z*=e.z,this}multiplyScalar(e){return this.x*=e,this.y*=e,this.z*=e,this}multiplyVectors(e,t){return this.x=e.x*t.x,this.y=e.y*t.y,this.z=e.z*t.z,this}applyEuler(e){return this.applyQuaternion(zx.setFromEuler(e))}applyAxisAngle(e,t){return this.applyQuaternion(zx.setFromAxisAngle(e,t))}applyMatrix3(e){let t=this.x,i=this.y,r=this.z,s=e.elements;return this.x=s[0]*t+s[3]*i+s[6]*r,this.y=s[1]*t+s[4]*i+s[7]*r,this.z=s[2]*t+s[5]*i+s[8]*r,this}applyNormalMatrix(e){return this.applyMatrix3(e).normalize()}applyMatrix4(e){let t=this.x,i=this.y,r=this.z,s=e.elements,o=1/(s[3]*t+s[7]*i+s[11]*r+s[15]);return this.x=(s[0]*t+s[4]*i+s[8]*r+s[12])*o,this.y=(s[1]*t+s[5]*i+s[9]*r+s[13])*o,this.z=(s[2]*t+s[6]*i+s[10]*r+s[14])*o,this}applyQuaternion(e){let t=this.x,i=this.y,r=this.z,s=e.x,o=e.y,a=e.z,c=e.w,l=2*(o*r-a*i),u=2*(a*t-s*r),f=2*(s*i-o*t);return this.x=t+c*l+o*f-a*u,this.y=i+c*u+a*l-s*f,this.z=r+c*f+s*u-o*l,this}project(e){return this.applyMatrix4(e.matrixWorldInverse).applyMatrix4(e.projectionMatrix)}unproject(e){return this.applyMatrix4(e.projectionMatrixInverse).applyMatrix4(e.matrixWorld)}transformDirection(e){let t=this.x,i=this.y,r=this.z,s=e.elements;return this.x=s[0]*t+s[4]*i+s[8]*r,this.y=s[1]*t+s[5]*i+s[9]*r,this.z=s[2]*t+s[6]*i+s[10]*r,this.normalize()}divide(e){return this.x/=e.x,this.y/=e.y,this.z/=e.z,this}divideScalar(e){return this.multiplyScalar(1/e)}min(e){return this.x=Math.min(this.x,e.x),this.y=Math.min(this.y,e.y),this.z=Math.min(this.z,e.z),this}max(e){return this.x=Math.max(this.x,e.x),this.y=Math.max(this.y,e.y),this.z=Math.max(this.z,e.z),this}clamp(e,t){return this.x=Ze(this.x,e.x,t.x),this.y=Ze(this.y,e.y,t.y),this.z=Ze(this.z,e.z,t.z),this}clampScalar(e,t){return this.x=Ze(this.x,e,t),this.y=Ze(this.y,e,t),this.z=Ze(this.z,e,t),this}clampLength(e,t){let i=this.length();return this.divideScalar(i||1).multiplyScalar(Ze(i,e,t))}floor(){return this.x=Math.floor(this.x),this.y=Math.floor(this.y),this.z=Math.floor(this.z),this}ceil(){return this.x=Math.ceil(this.x),this.y=Math.ceil(this.y),this.z=Math.ceil(this.z),this}round(){return this.x=Math.round(this.x),this.y=Math.round(this.y),this.z=Math.round(this.z),this}roundToZero(){return this.x=Math.trunc(this.x),this.y=Math.trunc(this.y),this.z=Math.trunc(this.z),this}negate(){return this.x=-this.x,this.y=-this.y,this.z=-this.z,this}dot(e){return this.x*e.x+this.y*e.y+this.z*e.z}lengthSq(){return this.x*this.x+this.y*this.y+this.z*this.z}length(){return Math.sqrt(this.x*this.x+this.y*this.y+this.z*this.z)}manhattanLength(){return Math.abs(this.x)+Math.abs(this.y)+Math.abs(this.z)}normalize(){return this.divideScalar(this.length()||1)}setLength(e){return this.normalize().multiplyScalar(e)}lerp(e,t){return this.x+=(e.x-this.x)*t,this.y+=(e.y-this.y)*t,this.z+=(e.z-this.z)*t,this}lerpVectors(e,t,i){return this.x=e.x+(t.x-e.x)*i,this.y=e.y+(t.y-e.y)*i,this.z=e.z+(t.z-e.z)*i,this}cross(e){return this.crossVectors(this,e)}crossVectors(e,t){let i=e.x,r=e.y,s=e.z,o=t.x,a=t.y,c=t.z;return this.x=r*c-s*a,this.y=s*o-i*c,this.z=i*a-r*o,this}projectOnVector(e){let t=e.lengthSq();if(t===0)return this.set(0,0,0);let i=e.dot(this)/t;return this.copy(e).multiplyScalar(i)}projectOnPlane(e){return um.copy(this).projectOnVector(e),this.sub(um)}reflect(e){return this.sub(um.copy(e).multiplyScalar(2*this.dot(e)))}angleTo(e){let t=Math.sqrt(this.lengthSq()*e.lengthSq());if(t===0)return Math.PI/2;let i=this.dot(e)/t;return Math.acos(Ze(i,-1,1))}distanceTo(e){return Math.sqrt(this.distanceToSquared(e))}distanceToSquared(e){let t=this.x-e.x,i=this.y-e.y,r=this.z-e.z;return t*t+i*i+r*r}manhattanDistanceTo(e){return Math.abs(this.x-e.x)+Math.abs(this.y-e.y)+Math.abs(this.z-e.z)}setFromSpherical(e){return this.setFromSphericalCoords(e.radius,e.phi,e.theta)}setFromSphericalCoords(e,t,i){let r=Math.sin(t)*e;return this.x=r*Math.sin(i),this.y=Math.cos(t)*e,this.z=r*Math.cos(i),this}setFromCylindrical(e){return this.setFromCylindricalCoords(e.radius,e.theta,e.y)}setFromCylindricalCoords(e,t,i){return this.x=e*Math.sin(t),this.y=i,this.z=e*Math.cos(t),this}setFromMatrixPosition(e){let t=e.elements;return this.x=t[12],this.y=t[13],this.z=t[14],this}setFromMatrixScale(e){let t=this.setFromMatrixColumn(e,0).length(),i=this.setFromMatrixColumn(e,1).length(),r=this.setFromMatrixColumn(e,2).length();return this.x=t,this.y=i,this.z=r,this}setFromMatrixColumn(e,t){return this.fromArray(e.elements,t*4)}setFromMatrix3Column(e,t){return this.fromArray(e.elements,t*3)}setFromEuler(e){return this.x=e._x,this.y=e._y,this.z=e._z,this}setFromColor(e){return this.x=e.r,this.y=e.g,this.z=e.b,this}equals(e){return e.x===this.x&&e.y===this.y&&e.z===this.z}fromArray(e,t=0){return this.x=e[t],this.y=e[t+1],this.z=e[t+2],this}toArray(e=[],t=0){return e[t]=this.x,e[t+1]=this.y,e[t+2]=this.z,e}fromBufferAttribute(e,t){return this.x=e.getX(t),this.y=e.getY(t),this.z=e.getZ(t),this}random(){return this.x=Math.random(),this.y=Math.random(),this.z=Math.random(),this}randomDirection(){let e=Math.random()*Math.PI*2,t=Math.random()*2-1,i=Math.sqrt(1-t*t);return this.x=i*Math.cos(e),this.y=t,this.z=i*Math.sin(e),this}*[Symbol.iterator](){yield this.x,yield this.y,yield this.z}},um=new k,zx=new wi,Be=class n{constructor(e,t,i,r,s,o,a,c,l){n.prototype.isMatrix3=!0,this.elements=[1,0,0,0,1,0,0,0,1],e!==void 0&&this.set(e,t,i,r,s,o,a,c,l)}set(e,t,i,r,s,o,a,c,l){let u=this.elements;return u[0]=e,u[1]=r,u[2]=a,u[3]=t,u[4]=s,u[5]=c,u[6]=i,u[7]=o,u[8]=l,this}identity(){return this.set(1,0,0,0,1,0,0,0,1),this}copy(e){let t=this.elements,i=e.elements;return t[0]=i[0],t[1]=i[1],t[2]=i[2],t[3]=i[3],t[4]=i[4],t[5]=i[5],t[6]=i[6],t[7]=i[7],t[8]=i[8],this}extractBasis(e,t,i){return e.setFromMatrix3Column(this,0),t.setFromMatrix3Column(this,1),i.setFromMatrix3Column(this,2),this}setFromMatrix4(e){let t=e.elements;return this.set(t[0],t[4],t[8],t[1],t[5],t[9],t[2],t[6],t[10]),this}multiply(e){return this.multiplyMatrices(this,e)}premultiply(e){return this.multiplyMatrices(e,this)}multiplyMatrices(e,t){let i=e.elements,r=t.elements,s=this.elements,o=i[0],a=i[3],c=i[6],l=i[1],u=i[4],f=i[7],d=i[2],h=i[5],g=i[8],x=r[0],m=r[3],p=r[6],M=r[1],T=r[4],E=r[7],A=r[2],I=r[5],D=r[8];return s[0]=o*x+a*M+c*A,s[3]=o*m+a*T+c*I,s[6]=o*p+a*E+c*D,s[1]=l*x+u*M+f*A,s[4]=l*m+u*T+f*I,s[7]=l*p+u*E+f*D,s[2]=d*x+h*M+g*A,s[5]=d*m+h*T+g*I,s[8]=d*p+h*E+g*D,this}multiplyScalar(e){let t=this.elements;return t[0]*=e,t[3]*=e,t[6]*=e,t[1]*=e,t[4]*=e,t[7]*=e,t[2]*=e,t[5]*=e,t[8]*=e,this}determinant(){let e=this.elements,t=e[0],i=e[1],r=e[2],s=e[3],o=e[4],a=e[5],c=e[6],l=e[7],u=e[8];return t*o*u-t*a*l-i*s*u+i*a*c+r*s*l-r*o*c}invert(){let e=this.elements,t=e[0],i=e[1],r=e[2],s=e[3],o=e[4],a=e[5],c=e[6],l=e[7],u=e[8],f=u*o-a*l,d=a*c-u*s,h=l*s-o*c,g=t*f+i*d+r*h;if(g===0)return this.set(0,0,0,0,0,0,0,0,0);let x=1/g;return e[0]=f*x,e[1]=(r*l-u*i)*x,e[2]=(a*i-r*o)*x,e[3]=d*x,e[4]=(u*t-r*c)*x,e[5]=(r*s-a*t)*x,e[6]=h*x,e[7]=(i*c-l*t)*x,e[8]=(o*t-i*s)*x,this}transpose(){let e,t=this.elements;return e=t[1],t[1]=t[3],t[3]=e,e=t[2],t[2]=t[6],t[6]=e,e=t[5],t[5]=t[7],t[7]=e,this}getNormalMatrix(e){return this.setFromMatrix4(e).invert().transpose()}transposeIntoArray(e){let t=this.elements;return e[0]=t[0],e[1]=t[3],e[2]=t[6],e[3]=t[1],e[4]=t[4],e[5]=t[7],e[6]=t[2],e[7]=t[5],e[8]=t[8],this}setUvTransform(e,t,i,r,s,o,a){let c=Math.cos(s),l=Math.sin(s);return this.set(i*c,i*l,-i*(c*o+l*a)+o+e,-r*l,r*c,-r*(-l*o+c*a)+a+t,0,0,1),this}scale(e,t){return this.premultiply(dm.makeScale(e,t)),this}rotate(e){return this.premultiply(dm.makeRotation(-e)),this}translate(e,t){return this.premultiply(dm.makeTranslation(e,t)),this}makeTranslation(e,t){return e.isVector2?this.set(1,0,e.x,0,1,e.y,0,0,1):this.set(1,0,e,0,1,t,0,0,1),this}makeRotation(e){let t=Math.cos(e),i=Math.sin(e);return this.set(t,-i,0,i,t,0,0,0,1),this}makeScale(e,t){return this.set(e,0,0,0,t,0,0,0,1),this}equals(e){let t=this.elements,i=e.elements;for(let r=0;r<9;r++)if(t[r]!==i[r])return!1;return!0}fromArray(e,t=0){for(let i=0;i<9;i++)this.elements[i]=e[i+t];return this}toArray(e=[],t=0){let i=this.elements;return e[t]=i[0],e[t+1]=i[1],e[t+2]=i[2],e[t+3]=i[3],e[t+4]=i[4],e[t+5]=i[5],e[t+6]=i[6],e[t+7]=i[7],e[t+8]=i[8],e}clone(){return new this.constructor().fromArray(this.elements)}},dm=new Be,Gx=new Be().set(.4123908,.3575843,.1804808,.212639,.7151687,.0721923,.0193308,.1191948,.9505322),Wx=new Be().set(3.2409699,-1.5373832,-.4986108,-.9692436,1.8759675,.0415551,.0556301,-.203977,1.0569715);function fC(){let n={enabled:!0,workingColorSpace:Ms,spaces:{},convert:function(r,s,o){return this.enabled===!1||s===o||!s||!o||(this.spaces[s].transfer===ut&&(r.r=Qi(r.r),r.g=Qi(r.g),r.b=Qi(r.b)),this.spaces[s].primaries!==this.spaces[o].primaries&&(r.applyMatrix3(this.spaces[s].toXYZ),r.applyMatrix3(this.spaces[o].fromXYZ)),this.spaces[o].transfer===ut&&(r.r=Mo(r.r),r.g=Mo(r.g),r.b=Mo(r.b))),r},workingToColorSpace:function(r,s){return this.convert(r,this.workingColorSpace,s)},colorSpaceToWorking:function(r,s){return this.convert(r,s,this.workingColorSpace)},getPrimaries:function(r){return this.spaces[r].primaries},getTransfer:function(r){return r===ir?Aa:this.spaces[r].transfer},getToneMappingMode:function(r){return this.spaces[r].outputColorSpaceConfig.toneMappingMode||"standard"},getLuminanceCoefficients:function(r,s=this.workingColorSpace){return r.fromArray(this.spaces[s].luminanceCoefficients)},define:function(r){Object.assign(this.spaces,r)},_getMatrix:function(r,s,o){return r.copy(this.spaces[s].toXYZ).multiply(this.spaces[o].fromXYZ)},_getDrawingBufferColorSpace:function(r){return this.spaces[r].outputColorSpaceConfig.drawingBufferColorSpace},_getUnpackColorSpace:function(r=this.workingColorSpace){return this.spaces[r].workingColorSpaceConfig.unpackColorSpace},fromWorkingColorSpace:function(r,s){return Na("ColorManagement: .fromWorkingColorSpace() has been renamed to .workingToColorSpace()."),n.workingToColorSpace(r,s)},toWorkingColorSpace:function(r,s){return Na("ColorManagement: .toWorkingColorSpace() has been renamed to .colorSpaceToWorking()."),n.colorSpaceToWorking(r,s)}},e=[.64,.33,.3,.6,.15,.06],t=[.2126,.7152,.0722],i=[.3127,.329];return n.define({[Ms]:{primaries:e,whitePoint:i,transfer:Aa,toXYZ:Gx,fromXYZ:Wx,luminanceCoefficients:t,workingColorSpaceConfig:{unpackColorSpace:Dn},outputColorSpaceConfig:{drawingBufferColorSpace:Dn}},[Dn]:{primaries:e,whitePoint:i,transfer:ut,toXYZ:Gx,fromXYZ:Wx,luminanceCoefficients:t,outputColorSpaceConfig:{drawingBufferColorSpace:Dn}}}),n}var tt=fC();function Qi(n){return n<.04045?n*.0773993808:Math.pow(n*.9478672986+.0521327014,2.4)}function Mo(n){return n<.0031308?n*12.92:1.055*Math.pow(n,.41666)-.055}var co,Cu=class{static getDataURL(e,t="image/png"){if(/^data:/i.test(e.src)||typeof HTMLCanvasElement>"u")return e.src;let i;if(e instanceof HTMLCanvasElement)i=e;else{co===void 0&&(co=Ra("canvas")),co.width=e.width,co.height=e.height;let r=co.getContext("2d");e instanceof ImageData?r.putImageData(e,0,0):r.drawImage(e,0,0,e.width,e.height),i=co}return i.toDataURL(t)}static sRGBToLinear(e){if(typeof HTMLImageElement<"u"&&e instanceof HTMLImageElement||typeof HTMLCanvasElement<"u"&&e instanceof HTMLCanvasElement||typeof ImageBitmap<"u"&&e instanceof ImageBitmap){let t=Ra("canvas");t.width=e.width,t.height=e.height;let i=t.getContext("2d");i.drawImage(e,0,0,e.width,e.height);let r=i.getImageData(0,0,e.width,e.height),s=r.data;for(let o=0;o<s.length;o++)s[o]=Qi(s[o]/255)*255;return i.putImageData(r,0,0),t}else if(e.data){let t=e.data.slice(0);for(let i=0;i<t.length;i++)t instanceof Uint8Array||t instanceof Uint8ClampedArray?t[i]=Math.floor(Qi(t[i]/255)*255):t[i]=Qi(t[i]);return{data:t,width:e.width,height:e.height}}else return Ne("ImageUtils.sRGBToLinear(): Unsupported image type. No color space conversion applied."),e}},hC=0,So=class{constructor(e=null){this.isSource=!0,Object.defineProperty(this,"id",{value:hC++}),this.uuid=nc(),this.data=e,this.dataReady=!0,this.version=0}getSize(e){let t=this.data;return typeof HTMLVideoElement<"u"&&t instanceof HTMLVideoElement?e.set(t.videoWidth,t.videoHeight,0):typeof VideoFrame<"u"&&t instanceof VideoFrame?e.set(t.displayHeight,t.displayWidth,0):t!==null?e.set(t.width,t.height,t.depth||0):e.set(0,0,0),e}set needsUpdate(e){e===!0&&this.version++}toJSON(e){let t=e===void 0||typeof e=="string";if(!t&&e.images[this.uuid]!==void 0)return e.images[this.uuid];let i={uuid:this.uuid,url:""},r=this.data;if(r!==null){let s;if(Array.isArray(r)){s=[];for(let o=0,a=r.length;o<a;o++)r[o].isDataTexture?s.push(fm(r[o].image)):s.push(fm(r[o]))}else s=fm(r);i.url=s}return t||(e.images[this.uuid]=i),i}};function fm(n){return typeof HTMLImageElement<"u"&&n instanceof HTMLImageElement||typeof HTMLCanvasElement<"u"&&n instanceof HTMLCanvasElement||typeof ImageBitmap<"u"&&n instanceof ImageBitmap?Cu.getDataURL(n):n.data?{data:Array.from(n.data),width:n.width,height:n.height,type:n.data.constructor.name}:(Ne("Texture: Unable to serialize Texture."),{})}var pC=0,hm=new k,rr=(()=>{class n extends nr{constructor(t=n.DEFAULT_IMAGE,i=n.DEFAULT_MAPPING,r=Ei,s=Ei,o=tn,a=Or,c=Yn,l=On,u=n.DEFAULT_ANISOTROPY,f=ir){super(),this.isTexture=!0,Object.defineProperty(this,"id",{value:pC++}),this.uuid=nc(),this.name="",this.source=new So(t),this.mipmaps=[],this.mapping=i,this.channel=0,this.wrapS=r,this.wrapT=s,this.magFilter=o,this.minFilter=a,this.anisotropy=u,this.format=c,this.internalFormat=null,this.type=l,this.offset=new ht(0,0),this.repeat=new ht(1,1),this.center=new ht(0,0),this.rotation=0,this.matrixAutoUpdate=!0,this.matrix=new Be,this.generateMipmaps=!0,this.premultiplyAlpha=!1,this.flipY=!0,this.unpackAlignment=4,this.colorSpace=f,this.userData={},this.updateRanges=[],this.version=0,this.onUpdate=null,this.renderTarget=null,this.isRenderTargetTexture=!1,this.isArrayTexture=!!(t&&t.depth&&t.depth>1),this.pmremVersion=0}get width(){return this.source.getSize(hm).x}get height(){return this.source.getSize(hm).y}get depth(){return this.source.getSize(hm).z}get image(){return this.source.data}set image(t=null){this.source.data=t}updateMatrix(){this.matrix.setUvTransform(this.offset.x,this.offset.y,this.repeat.x,this.repeat.y,this.rotation,this.center.x,this.center.y)}addUpdateRange(t,i){this.updateRanges.push({start:t,count:i})}clearUpdateRanges(){this.updateRanges.length=0}clone(){return new this.constructor().copy(this)}copy(t){return this.name=t.name,this.source=t.source,this.mipmaps=t.mipmaps.slice(0),this.mapping=t.mapping,this.channel=t.channel,this.wrapS=t.wrapS,this.wrapT=t.wrapT,this.magFilter=t.magFilter,this.minFilter=t.minFilter,this.anisotropy=t.anisotropy,this.format=t.format,this.internalFormat=t.internalFormat,this.type=t.type,this.offset.copy(t.offset),this.repeat.copy(t.repeat),this.center.copy(t.center),this.rotation=t.rotation,this.matrixAutoUpdate=t.matrixAutoUpdate,this.matrix.copy(t.matrix),this.generateMipmaps=t.generateMipmaps,this.premultiplyAlpha=t.premultiplyAlpha,this.flipY=t.flipY,this.unpackAlignment=t.unpackAlignment,this.colorSpace=t.colorSpace,this.renderTarget=t.renderTarget,this.isRenderTargetTexture=t.isRenderTargetTexture,this.isArrayTexture=t.isArrayTexture,this.userData=JSON.parse(JSON.stringify(t.userData)),this.needsUpdate=!0,this}setValues(t){for(let i in t){let r=t[i];if(r===void 0){Ne(`Texture.setValues(): parameter '${i}' has value of undefined.`);continue}let s=this[i];if(s===void 0){Ne(`Texture.setValues(): property '${i}' does not exist.`);continue}s&&r&&s.isVector2&&r.isVector2||s&&r&&s.isVector3&&r.isVector3||s&&r&&s.isMatrix3&&r.isMatrix3?s.copy(r):this[i]=r}}toJSON(t){let i=t===void 0||typeof t=="string";if(!i&&t.textures[this.uuid]!==void 0)return t.textures[this.uuid];let r={metadata:{version:4.7,type:"Texture",generator:"Texture.toJSON"},uuid:this.uuid,name:this.name,image:this.source.toJSON(t).uuid,mapping:this.mapping,channel:this.channel,repeat:[this.repeat.x,this.repeat.y],offset:[this.offset.x,this.offset.y],center:[this.center.x,this.center.y],rotation:this.rotation,wrap:[this.wrapS,this.wrapT],format:this.format,internalFormat:this.internalFormat,type:this.type,colorSpace:this.colorSpace,minFilter:this.minFilter,magFilter:this.magFilter,anisotropy:this.anisotropy,flipY:this.flipY,generateMipmaps:this.generateMipmaps,premultiplyAlpha:this.premultiplyAlpha,unpackAlignment:this.unpackAlignment};return Object.keys(this.userData).length>0&&(r.userData=this.userData),i||(t.textures[this.uuid]=r),r}dispose(){this.dispatchEvent({type:"dispose"})}transformUv(t){if(this.mapping!==Rm)return t;if(t.applyMatrix3(this.matrix),t.x<0||t.x>1)switch(this.wrapS){case Su:t.x=t.x-Math.floor(t.x);break;case Ei:t.x=t.x<0?0:1;break;case Eu:Math.abs(Math.floor(t.x)%2)===1?t.x=Math.ceil(t.x)-t.x:t.x=t.x-Math.floor(t.x);break}if(t.y<0||t.y>1)switch(this.wrapT){case Su:t.y=t.y-Math.floor(t.y);break;case Ei:t.y=t.y<0?0:1;break;case Eu:Math.abs(Math.floor(t.y)%2)===1?t.y=Math.ceil(t.y)-t.y:t.y=t.y-Math.floor(t.y);break}return this.flipY&&(t.y=1-t.y),t}set needsUpdate(t){t===!0&&(this.version++,this.source.needsUpdate=!0)}set needsPMREMUpdate(t){t===!0&&this.pmremVersion++}}return n.DEFAULT_IMAGE=null,n.DEFAULT_MAPPING=Rm,n.DEFAULT_ANISOTROPY=1,n})(),Pt=class n{constructor(e=0,t=0,i=0,r=1){n.prototype.isVector4=!0,this.x=e,this.y=t,this.z=i,this.w=r}get width(){return this.z}set width(e){this.z=e}get height(){return this.w}set height(e){this.w=e}set(e,t,i,r){return this.x=e,this.y=t,this.z=i,this.w=r,this}setScalar(e){return this.x=e,this.y=e,this.z=e,this.w=e,this}setX(e){return this.x=e,this}setY(e){return this.y=e,this}setZ(e){return this.z=e,this}setW(e){return this.w=e,this}setComponent(e,t){switch(e){case 0:this.x=t;break;case 1:this.y=t;break;case 2:this.z=t;break;case 3:this.w=t;break;default:throw new Error("index is out of range: "+e)}return this}getComponent(e){switch(e){case 0:return this.x;case 1:return this.y;case 2:return this.z;case 3:return this.w;default:throw new Error("index is out of range: "+e)}}clone(){return new this.constructor(this.x,this.y,this.z,this.w)}copy(e){return this.x=e.x,this.y=e.y,this.z=e.z,this.w=e.w!==void 0?e.w:1,this}add(e){return this.x+=e.x,this.y+=e.y,this.z+=e.z,this.w+=e.w,this}addScalar(e){return this.x+=e,this.y+=e,this.z+=e,this.w+=e,this}addVectors(e,t){return this.x=e.x+t.x,this.y=e.y+t.y,this.z=e.z+t.z,this.w=e.w+t.w,this}addScaledVector(e,t){return this.x+=e.x*t,this.y+=e.y*t,this.z+=e.z*t,this.w+=e.w*t,this}sub(e){return this.x-=e.x,this.y-=e.y,this.z-=e.z,this.w-=e.w,this}subScalar(e){return this.x-=e,this.y-=e,this.z-=e,this.w-=e,this}subVectors(e,t){return this.x=e.x-t.x,this.y=e.y-t.y,this.z=e.z-t.z,this.w=e.w-t.w,this}multiply(e){return this.x*=e.x,this.y*=e.y,this.z*=e.z,this.w*=e.w,this}multiplyScalar(e){return this.x*=e,this.y*=e,this.z*=e,this.w*=e,this}applyMatrix4(e){let t=this.x,i=this.y,r=this.z,s=this.w,o=e.elements;return this.x=o[0]*t+o[4]*i+o[8]*r+o[12]*s,this.y=o[1]*t+o[5]*i+o[9]*r+o[13]*s,this.z=o[2]*t+o[6]*i+o[10]*r+o[14]*s,this.w=o[3]*t+o[7]*i+o[11]*r+o[15]*s,this}divide(e){return this.x/=e.x,this.y/=e.y,this.z/=e.z,this.w/=e.w,this}divideScalar(e){return this.multiplyScalar(1/e)}setAxisAngleFromQuaternion(e){this.w=2*Math.acos(e.w);let t=Math.sqrt(1-e.w*e.w);return t<1e-4?(this.x=1,this.y=0,this.z=0):(this.x=e.x/t,this.y=e.y/t,this.z=e.z/t),this}setAxisAngleFromRotationMatrix(e){let t,i,r,s,c=e.elements,l=c[0],u=c[4],f=c[8],d=c[1],h=c[5],g=c[9],x=c[2],m=c[6],p=c[10];if(Math.abs(u-d)<.01&&Math.abs(f-x)<.01&&Math.abs(g-m)<.01){if(Math.abs(u+d)<.1&&Math.abs(f+x)<.1&&Math.abs(g+m)<.1&&Math.abs(l+h+p-3)<.1)return this.set(1,0,0,0),this;t=Math.PI;let T=(l+1)/2,E=(h+1)/2,A=(p+1)/2,I=(u+d)/4,D=(f+x)/4,y=(g+m)/4;return T>E&&T>A?T<.01?(i=0,r=.707106781,s=.707106781):(i=Math.sqrt(T),r=I/i,s=D/i):E>A?E<.01?(i=.707106781,r=0,s=.707106781):(r=Math.sqrt(E),i=I/r,s=y/r):A<.01?(i=.707106781,r=.707106781,s=0):(s=Math.sqrt(A),i=D/s,r=y/s),this.set(i,r,s,t),this}let M=Math.sqrt((m-g)*(m-g)+(f-x)*(f-x)+(d-u)*(d-u));return Math.abs(M)<.001&&(M=1),this.x=(m-g)/M,this.y=(f-x)/M,this.z=(d-u)/M,this.w=Math.acos((l+h+p-1)/2),this}setFromMatrixPosition(e){let t=e.elements;return this.x=t[12],this.y=t[13],this.z=t[14],this.w=t[15],this}min(e){return this.x=Math.min(this.x,e.x),this.y=Math.min(this.y,e.y),this.z=Math.min(this.z,e.z),this.w=Math.min(this.w,e.w),this}max(e){return this.x=Math.max(this.x,e.x),this.y=Math.max(this.y,e.y),this.z=Math.max(this.z,e.z),this.w=Math.max(this.w,e.w),this}clamp(e,t){return this.x=Ze(this.x,e.x,t.x),this.y=Ze(this.y,e.y,t.y),this.z=Ze(this.z,e.z,t.z),this.w=Ze(this.w,e.w,t.w),this}clampScalar(e,t){return this.x=Ze(this.x,e,t),this.y=Ze(this.y,e,t),this.z=Ze(this.z,e,t),this.w=Ze(this.w,e,t),this}clampLength(e,t){let i=this.length();return this.divideScalar(i||1).multiplyScalar(Ze(i,e,t))}floor(){return this.x=Math.floor(this.x),this.y=Math.floor(this.y),this.z=Math.floor(this.z),this.w=Math.floor(this.w),this}ceil(){return this.x=Math.ceil(this.x),this.y=Math.ceil(this.y),this.z=Math.ceil(this.z),this.w=Math.ceil(this.w),this}round(){return this.x=Math.round(this.x),this.y=Math.round(this.y),this.z=Math.round(this.z),this.w=Math.round(this.w),this}roundToZero(){return this.x=Math.trunc(this.x),this.y=Math.trunc(this.y),this.z=Math.trunc(this.z),this.w=Math.trunc(this.w),this}negate(){return this.x=-this.x,this.y=-this.y,this.z=-this.z,this.w=-this.w,this}dot(e){return this.x*e.x+this.y*e.y+this.z*e.z+this.w*e.w}lengthSq(){return this.x*this.x+this.y*this.y+this.z*this.z+this.w*this.w}length(){return Math.sqrt(this.x*this.x+this.y*this.y+this.z*this.z+this.w*this.w)}manhattanLength(){return Math.abs(this.x)+Math.abs(this.y)+Math.abs(this.z)+Math.abs(this.w)}normalize(){return this.divideScalar(this.length()||1)}setLength(e){return this.normalize().multiplyScalar(e)}lerp(e,t){return this.x+=(e.x-this.x)*t,this.y+=(e.y-this.y)*t,this.z+=(e.z-this.z)*t,this.w+=(e.w-this.w)*t,this}lerpVectors(e,t,i){return this.x=e.x+(t.x-e.x)*i,this.y=e.y+(t.y-e.y)*i,this.z=e.z+(t.z-e.z)*i,this.w=e.w+(t.w-e.w)*i,this}equals(e){return e.x===this.x&&e.y===this.y&&e.z===this.z&&e.w===this.w}fromArray(e,t=0){return this.x=e[t],this.y=e[t+1],this.z=e[t+2],this.w=e[t+3],this}toArray(e=[],t=0){return e[t]=this.x,e[t+1]=this.y,e[t+2]=this.z,e[t+3]=this.w,e}fromBufferAttribute(e,t){return this.x=e.getX(t),this.y=e.getY(t),this.z=e.getZ(t),this.w=e.getW(t),this}random(){return this.x=Math.random(),this.y=Math.random(),this.z=Math.random(),this.w=Math.random(),this}*[Symbol.iterator](){yield this.x,yield this.y,yield this.z,yield this.w}},Iu=class extends nr{constructor(e=1,t=1,i={}){super(),i=Object.assign({generateMipmaps:!1,internalFormat:null,minFilter:tn,depthBuffer:!0,stencilBuffer:!1,resolveDepthBuffer:!0,resolveStencilBuffer:!0,depthTexture:null,samples:0,count:1,depth:1,multiview:!1},i),this.isRenderTarget=!0,this.width=e,this.height=t,this.depth=i.depth,this.scissor=new Pt(0,0,e,t),this.scissorTest=!1,this.viewport=new Pt(0,0,e,t),this.textures=[];let r={width:e,height:t,depth:i.depth},s=new rr(r),o=i.count;for(let a=0;a<o;a++)this.textures[a]=s.clone(),this.textures[a].isRenderTargetTexture=!0,this.textures[a].renderTarget=this;this._setTextureOptions(i),this.depthBuffer=i.depthBuffer,this.stencilBuffer=i.stencilBuffer,this.resolveDepthBuffer=i.resolveDepthBuffer,this.resolveStencilBuffer=i.resolveStencilBuffer,this._depthTexture=null,this.depthTexture=i.depthTexture,this.samples=i.samples,this.multiview=i.multiview}_setTextureOptions(e={}){let t={minFilter:tn,generateMipmaps:!1,flipY:!1,internalFormat:null};e.mapping!==void 0&&(t.mapping=e.mapping),e.wrapS!==void 0&&(t.wrapS=e.wrapS),e.wrapT!==void 0&&(t.wrapT=e.wrapT),e.wrapR!==void 0&&(t.wrapR=e.wrapR),e.magFilter!==void 0&&(t.magFilter=e.magFilter),e.minFilter!==void 0&&(t.minFilter=e.minFilter),e.format!==void 0&&(t.format=e.format),e.type!==void 0&&(t.type=e.type),e.anisotropy!==void 0&&(t.anisotropy=e.anisotropy),e.colorSpace!==void 0&&(t.colorSpace=e.colorSpace),e.flipY!==void 0&&(t.flipY=e.flipY),e.generateMipmaps!==void 0&&(t.generateMipmaps=e.generateMipmaps),e.internalFormat!==void 0&&(t.internalFormat=e.internalFormat);for(let i=0;i<this.textures.length;i++)this.textures[i].setValues(t)}get texture(){return this.textures[0]}set texture(e){this.textures[0]=e}set depthTexture(e){this._depthTexture!==null&&(this._depthTexture.renderTarget=null),e!==null&&(e.renderTarget=this),this._depthTexture=e}get depthTexture(){return this._depthTexture}setSize(e,t,i=1){if(this.width!==e||this.height!==t||this.depth!==i){this.width=e,this.height=t,this.depth=i;for(let r=0,s=this.textures.length;r<s;r++)this.textures[r].image.width=e,this.textures[r].image.height=t,this.textures[r].image.depth=i,this.textures[r].isData3DTexture!==!0&&(this.textures[r].isArrayTexture=this.textures[r].image.depth>1);this.dispose()}this.viewport.set(0,0,e,t),this.scissor.set(0,0,e,t)}clone(){return new this.constructor().copy(this)}copy(e){this.width=e.width,this.height=e.height,this.depth=e.depth,this.scissor.copy(e.scissor),this.scissorTest=e.scissorTest,this.viewport.copy(e.viewport),this.textures.length=0;for(let t=0,i=e.textures.length;t<i;t++){this.textures[t]=e.textures[t].clone(),this.textures[t].isRenderTargetTexture=!0,this.textures[t].renderTarget=this;let r=Object.assign({},e.textures[t].image);this.textures[t].source=new So(r)}return this.depthBuffer=e.depthBuffer,this.stencilBuffer=e.stencilBuffer,this.resolveDepthBuffer=e.resolveDepthBuffer,this.resolveStencilBuffer=e.resolveStencilBuffer,e.depthTexture!==null&&(this.depthTexture=e.depthTexture.clone()),this.samples=e.samples,this}dispose(){this.dispatchEvent({type:"dispose"})}},Rn=class extends Iu{constructor(e=1,t=1,i={}){super(e,t,i),this.isWebGLRenderTarget=!0}},Pa=class extends rr{constructor(e=null,t=1,i=1,r=1){super(null),this.isDataArrayTexture=!0,this.image={data:e,width:t,height:i,depth:r},this.magFilter=Kt,this.minFilter=Kt,this.wrapR=Ei,this.generateMipmaps=!1,this.flipY=!1,this.unpackAlignment=1,this.layerUpdates=new Set}addLayerUpdate(e){this.layerUpdates.add(e)}clearLayerUpdates(){this.layerUpdates.clear()}};var Au=class extends rr{constructor(e=null,t=1,i=1,r=1){super(null),this.isData3DTexture=!0,this.image={data:e,width:t,height:i,depth:r},this.magFilter=Kt,this.minFilter=Kt,this.wrapR=Ei,this.generateMipmaps=!1,this.flipY=!1,this.unpackAlignment=1}};var Ut=class n{constructor(e,t,i,r,s,o,a,c,l,u,f,d,h,g,x,m){n.prototype.isMatrix4=!0,this.elements=[1,0,0,0,0,1,0,0,0,0,1,0,0,0,0,1],e!==void 0&&this.set(e,t,i,r,s,o,a,c,l,u,f,d,h,g,x,m)}set(e,t,i,r,s,o,a,c,l,u,f,d,h,g,x,m){let p=this.elements;return p[0]=e,p[4]=t,p[8]=i,p[12]=r,p[1]=s,p[5]=o,p[9]=a,p[13]=c,p[2]=l,p[6]=u,p[10]=f,p[14]=d,p[3]=h,p[7]=g,p[11]=x,p[15]=m,this}identity(){return this.set(1,0,0,0,0,1,0,0,0,0,1,0,0,0,0,1),this}clone(){return new n().fromArray(this.elements)}copy(e){let t=this.elements,i=e.elements;return t[0]=i[0],t[1]=i[1],t[2]=i[2],t[3]=i[3],t[4]=i[4],t[5]=i[5],t[6]=i[6],t[7]=i[7],t[8]=i[8],t[9]=i[9],t[10]=i[10],t[11]=i[11],t[12]=i[12],t[13]=i[13],t[14]=i[14],t[15]=i[15],this}copyPosition(e){let t=this.elements,i=e.elements;return t[12]=i[12],t[13]=i[13],t[14]=i[14],this}setFromMatrix3(e){let t=e.elements;return this.set(t[0],t[3],t[6],0,t[1],t[4],t[7],0,t[2],t[5],t[8],0,0,0,0,1),this}extractBasis(e,t,i){return this.determinant()===0?(e.set(1,0,0),t.set(0,1,0),i.set(0,0,1),this):(e.setFromMatrixColumn(this,0),t.setFromMatrixColumn(this,1),i.setFromMatrixColumn(this,2),this)}makeBasis(e,t,i){return this.set(e.x,t.x,i.x,0,e.y,t.y,i.y,0,e.z,t.z,i.z,0,0,0,0,1),this}extractRotation(e){if(e.determinant()===0)return this.identity();let t=this.elements,i=e.elements,r=1/lo.setFromMatrixColumn(e,0).length(),s=1/lo.setFromMatrixColumn(e,1).length(),o=1/lo.setFromMatrixColumn(e,2).length();return t[0]=i[0]*r,t[1]=i[1]*r,t[2]=i[2]*r,t[3]=0,t[4]=i[4]*s,t[5]=i[5]*s,t[6]=i[6]*s,t[7]=0,t[8]=i[8]*o,t[9]=i[9]*o,t[10]=i[10]*o,t[11]=0,t[12]=0,t[13]=0,t[14]=0,t[15]=1,this}makeRotationFromEuler(e){let t=this.elements,i=e.x,r=e.y,s=e.z,o=Math.cos(i),a=Math.sin(i),c=Math.cos(r),l=Math.sin(r),u=Math.cos(s),f=Math.sin(s);if(e.order==="XYZ"){let d=o*u,h=o*f,g=a*u,x=a*f;t[0]=c*u,t[4]=-c*f,t[8]=l,t[1]=h+g*l,t[5]=d-x*l,t[9]=-a*c,t[2]=x-d*l,t[6]=g+h*l,t[10]=o*c}else if(e.order==="YXZ"){let d=c*u,h=c*f,g=l*u,x=l*f;t[0]=d+x*a,t[4]=g*a-h,t[8]=o*l,t[1]=o*f,t[5]=o*u,t[9]=-a,t[2]=h*a-g,t[6]=x+d*a,t[10]=o*c}else if(e.order==="ZXY"){let d=c*u,h=c*f,g=l*u,x=l*f;t[0]=d-x*a,t[4]=-o*f,t[8]=g+h*a,t[1]=h+g*a,t[5]=o*u,t[9]=x-d*a,t[2]=-o*l,t[6]=a,t[10]=o*c}else if(e.order==="ZYX"){let d=o*u,h=o*f,g=a*u,x=a*f;t[0]=c*u,t[4]=g*l-h,t[8]=d*l+x,t[1]=c*f,t[5]=x*l+d,t[9]=h*l-g,t[2]=-l,t[6]=a*c,t[10]=o*c}else if(e.order==="YZX"){let d=o*c,h=o*l,g=a*c,x=a*l;t[0]=c*u,t[4]=x-d*f,t[8]=g*f+h,t[1]=f,t[5]=o*u,t[9]=-a*u,t[2]=-l*u,t[6]=h*f+g,t[10]=d-x*f}else if(e.order==="XZY"){let d=o*c,h=o*l,g=a*c,x=a*l;t[0]=c*u,t[4]=-f,t[8]=l*u,t[1]=d*f+x,t[5]=o*u,t[9]=h*f-g,t[2]=g*f-h,t[6]=a*u,t[10]=x*f+d}return t[3]=0,t[7]=0,t[11]=0,t[12]=0,t[13]=0,t[14]=0,t[15]=1,this}makeRotationFromQuaternion(e){return this.compose(mC,e,gC)}lookAt(e,t,i){let r=this.elements;return In.subVectors(e,t),In.lengthSq()===0&&(In.z=1),In.normalize(),Mr.crossVectors(i,In),Mr.lengthSq()===0&&(Math.abs(i.z)===1?In.x+=1e-4:In.z+=1e-4,In.normalize(),Mr.crossVectors(i,In)),Mr.normalize(),jl.crossVectors(In,Mr),r[0]=Mr.x,r[4]=jl.x,r[8]=In.x,r[1]=Mr.y,r[5]=jl.y,r[9]=In.y,r[2]=Mr.z,r[6]=jl.z,r[10]=In.z,this}multiply(e){return this.multiplyMatrices(this,e)}premultiply(e){return this.multiplyMatrices(e,this)}multiplyMatrices(e,t){let i=e.elements,r=t.elements,s=this.elements,o=i[0],a=i[4],c=i[8],l=i[12],u=i[1],f=i[5],d=i[9],h=i[13],g=i[2],x=i[6],m=i[10],p=i[14],M=i[3],T=i[7],E=i[11],A=i[15],I=r[0],D=r[4],y=r[8],S=r[12],q=r[1],w=r[5],U=r[9],V=r[13],W=r[2],B=r[6],H=r[10],L=r[14],Q=r[3],Z=r[7],ue=r[11],ge=r[15];return s[0]=o*I+a*q+c*W+l*Q,s[4]=o*D+a*w+c*B+l*Z,s[8]=o*y+a*U+c*H+l*ue,s[12]=o*S+a*V+c*L+l*ge,s[1]=u*I+f*q+d*W+h*Q,s[5]=u*D+f*w+d*B+h*Z,s[9]=u*y+f*U+d*H+h*ue,s[13]=u*S+f*V+d*L+h*ge,s[2]=g*I+x*q+m*W+p*Q,s[6]=g*D+x*w+m*B+p*Z,s[10]=g*y+x*U+m*H+p*ue,s[14]=g*S+x*V+m*L+p*ge,s[3]=M*I+T*q+E*W+A*Q,s[7]=M*D+T*w+E*B+A*Z,s[11]=M*y+T*U+E*H+A*ue,s[15]=M*S+T*V+E*L+A*ge,this}multiplyScalar(e){let t=this.elements;return t[0]*=e,t[4]*=e,t[8]*=e,t[12]*=e,t[1]*=e,t[5]*=e,t[9]*=e,t[13]*=e,t[2]*=e,t[6]*=e,t[10]*=e,t[14]*=e,t[3]*=e,t[7]*=e,t[11]*=e,t[15]*=e,this}determinant(){let e=this.elements,t=e[0],i=e[4],r=e[8],s=e[12],o=e[1],a=e[5],c=e[9],l=e[13],u=e[2],f=e[6],d=e[10],h=e[14],g=e[3],x=e[7],m=e[11],p=e[15],M=c*h-l*d,T=a*h-l*f,E=a*d-c*f,A=o*h-l*u,I=o*d-c*u,D=o*f-a*u;return t*(x*M-m*T+p*E)-i*(g*M-m*A+p*I)+r*(g*T-x*A+p*D)-s*(g*E-x*I+m*D)}transpose(){let e=this.elements,t;return t=e[1],e[1]=e[4],e[4]=t,t=e[2],e[2]=e[8],e[8]=t,t=e[6],e[6]=e[9],e[9]=t,t=e[3],e[3]=e[12],e[12]=t,t=e[7],e[7]=e[13],e[13]=t,t=e[11],e[11]=e[14],e[14]=t,this}setPosition(e,t,i){let r=this.elements;return e.isVector3?(r[12]=e.x,r[13]=e.y,r[14]=e.z):(r[12]=e,r[13]=t,r[14]=i),this}invert(){let e=this.elements,t=e[0],i=e[1],r=e[2],s=e[3],o=e[4],a=e[5],c=e[6],l=e[7],u=e[8],f=e[9],d=e[10],h=e[11],g=e[12],x=e[13],m=e[14],p=e[15],M=t*a-i*o,T=t*c-r*o,E=t*l-s*o,A=i*c-r*a,I=i*l-s*a,D=r*l-s*c,y=u*x-f*g,S=u*m-d*g,q=u*p-h*g,w=f*m-d*x,U=f*p-h*x,V=d*p-h*m,W=M*V-T*U+E*w+A*q-I*S+D*y;if(W===0)return this.set(0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0);let B=1/W;return e[0]=(a*V-c*U+l*w)*B,e[1]=(r*U-i*V-s*w)*B,e[2]=(x*D-m*I+p*A)*B,e[3]=(d*I-f*D-h*A)*B,e[4]=(c*q-o*V-l*S)*B,e[5]=(t*V-r*q+s*S)*B,e[6]=(m*E-g*D-p*T)*B,e[7]=(u*D-d*E+h*T)*B,e[8]=(o*U-a*q+l*y)*B,e[9]=(i*q-t*U-s*y)*B,e[10]=(g*I-x*E+p*M)*B,e[11]=(f*E-u*I-h*M)*B,e[12]=(a*S-o*w-c*y)*B,e[13]=(t*w-i*S+r*y)*B,e[14]=(x*T-g*A-m*M)*B,e[15]=(u*A-f*T+d*M)*B,this}scale(e){let t=this.elements,i=e.x,r=e.y,s=e.z;return t[0]*=i,t[4]*=r,t[8]*=s,t[1]*=i,t[5]*=r,t[9]*=s,t[2]*=i,t[6]*=r,t[10]*=s,t[3]*=i,t[7]*=r,t[11]*=s,this}getMaxScaleOnAxis(){let e=this.elements,t=e[0]*e[0]+e[1]*e[1]+e[2]*e[2],i=e[4]*e[4]+e[5]*e[5]+e[6]*e[6],r=e[8]*e[8]+e[9]*e[9]+e[10]*e[10];return Math.sqrt(Math.max(t,i,r))}makeTranslation(e,t,i){return e.isVector3?this.set(1,0,0,e.x,0,1,0,e.y,0,0,1,e.z,0,0,0,1):this.set(1,0,0,e,0,1,0,t,0,0,1,i,0,0,0,1),this}makeRotationX(e){let t=Math.cos(e),i=Math.sin(e);return this.set(1,0,0,0,0,t,-i,0,0,i,t,0,0,0,0,1),this}makeRotationY(e){let t=Math.cos(e),i=Math.sin(e);return this.set(t,0,i,0,0,1,0,0,-i,0,t,0,0,0,0,1),this}makeRotationZ(e){let t=Math.cos(e),i=Math.sin(e);return this.set(t,-i,0,0,i,t,0,0,0,0,1,0,0,0,0,1),this}makeRotationAxis(e,t){let i=Math.cos(t),r=Math.sin(t),s=1-i,o=e.x,a=e.y,c=e.z,l=s*o,u=s*a;return this.set(l*o+i,l*a-r*c,l*c+r*a,0,l*a+r*c,u*a+i,u*c-r*o,0,l*c-r*a,u*c+r*o,s*c*c+i,0,0,0,0,1),this}makeScale(e,t,i){return this.set(e,0,0,0,0,t,0,0,0,0,i,0,0,0,0,1),this}makeShear(e,t,i,r,s,o){return this.set(1,i,s,0,e,1,o,0,t,r,1,0,0,0,0,1),this}compose(e,t,i){let r=this.elements,s=t._x,o=t._y,a=t._z,c=t._w,l=s+s,u=o+o,f=a+a,d=s*l,h=s*u,g=s*f,x=o*u,m=o*f,p=a*f,M=c*l,T=c*u,E=c*f,A=i.x,I=i.y,D=i.z;return r[0]=(1-(x+p))*A,r[1]=(h+E)*A,r[2]=(g-T)*A,r[3]=0,r[4]=(h-E)*I,r[5]=(1-(d+p))*I,r[6]=(m+M)*I,r[7]=0,r[8]=(g+T)*D,r[9]=(m-M)*D,r[10]=(1-(d+x))*D,r[11]=0,r[12]=e.x,r[13]=e.y,r[14]=e.z,r[15]=1,this}decompose(e,t,i){let r=this.elements;e.x=r[12],e.y=r[13],e.z=r[14];let s=this.determinant();if(s===0)return i.set(1,1,1),t.identity(),this;let o=lo.set(r[0],r[1],r[2]).length(),a=lo.set(r[4],r[5],r[6]).length(),c=lo.set(r[8],r[9],r[10]).length();s<0&&(o=-o),ai.copy(this);let l=1/o,u=1/a,f=1/c;return ai.elements[0]*=l,ai.elements[1]*=l,ai.elements[2]*=l,ai.elements[4]*=u,ai.elements[5]*=u,ai.elements[6]*=u,ai.elements[8]*=f,ai.elements[9]*=f,ai.elements[10]*=f,t.setFromRotationMatrix(ai),i.x=o,i.y=a,i.z=c,this}makePerspective(e,t,i,r,s,o,a=ui,c=!1){let l=this.elements,u=2*s/(t-e),f=2*s/(i-r),d=(t+e)/(t-e),h=(i+r)/(i-r),g,x;if(c)g=s/(o-s),x=o*s/(o-s);else if(a===ui)g=-(o+s)/(o-s),x=-2*o*s/(o-s);else if(a===Da)g=-o/(o-s),x=-o*s/(o-s);else throw new Error("THREE.Matrix4.makePerspective(): Invalid coordinate system: "+a);return l[0]=u,l[4]=0,l[8]=d,l[12]=0,l[1]=0,l[5]=f,l[9]=h,l[13]=0,l[2]=0,l[6]=0,l[10]=g,l[14]=x,l[3]=0,l[7]=0,l[11]=-1,l[15]=0,this}makeOrthographic(e,t,i,r,s,o,a=ui,c=!1){let l=this.elements,u=2/(t-e),f=2/(i-r),d=-(t+e)/(t-e),h=-(i+r)/(i-r),g,x;if(c)g=1/(o-s),x=o/(o-s);else if(a===ui)g=-2/(o-s),x=-(o+s)/(o-s);else if(a===Da)g=-1/(o-s),x=-s/(o-s);else throw new Error("THREE.Matrix4.makeOrthographic(): Invalid coordinate system: "+a);return l[0]=u,l[4]=0,l[8]=0,l[12]=d,l[1]=0,l[5]=f,l[9]=0,l[13]=h,l[2]=0,l[6]=0,l[10]=g,l[14]=x,l[3]=0,l[7]=0,l[11]=0,l[15]=1,this}equals(e){let t=this.elements,i=e.elements;for(let r=0;r<16;r++)if(t[r]!==i[r])return!1;return!0}fromArray(e,t=0){for(let i=0;i<16;i++)this.elements[i]=e[i+t];return this}toArray(e=[],t=0){let i=this.elements;return e[t]=i[0],e[t+1]=i[1],e[t+2]=i[2],e[t+3]=i[3],e[t+4]=i[4],e[t+5]=i[5],e[t+6]=i[6],e[t+7]=i[7],e[t+8]=i[8],e[t+9]=i[9],e[t+10]=i[10],e[t+11]=i[11],e[t+12]=i[12],e[t+13]=i[13],e[t+14]=i[14],e[t+15]=i[15],e}},lo=new k,ai=new Ut,mC=new k(0,0,0),gC=new k(1,1,1),Mr=new k,jl=new k,In=new k,jx=new Ut,$x=new wi,bs=(()=>{class n{constructor(t=0,i=0,r=0,s=n.DEFAULT_ORDER){this.isEuler=!0,this._x=t,this._y=i,this._z=r,this._order=s}get x(){return this._x}set x(t){this._x=t,this._onChangeCallback()}get y(){return this._y}set y(t){this._y=t,this._onChangeCallback()}get z(){return this._z}set z(t){this._z=t,this._onChangeCallback()}get order(){return this._order}set order(t){this._order=t,this._onChangeCallback()}set(t,i,r,s=this._order){return this._x=t,this._y=i,this._z=r,this._order=s,this._onChangeCallback(),this}clone(){return new this.constructor(this._x,this._y,this._z,this._order)}copy(t){return this._x=t._x,this._y=t._y,this._z=t._z,this._order=t._order,this._onChangeCallback(),this}setFromRotationMatrix(t,i=this._order,r=!0){let s=t.elements,o=s[0],a=s[4],c=s[8],l=s[1],u=s[5],f=s[9],d=s[2],h=s[6],g=s[10];switch(i){case"XYZ":this._y=Math.asin(Ze(c,-1,1)),Math.abs(c)<.9999999?(this._x=Math.atan2(-f,g),this._z=Math.atan2(-a,o)):(this._x=Math.atan2(h,u),this._z=0);break;case"YXZ":this._x=Math.asin(-Ze(f,-1,1)),Math.abs(f)<.9999999?(this._y=Math.atan2(c,g),this._z=Math.atan2(l,u)):(this._y=Math.atan2(-d,o),this._z=0);break;case"ZXY":this._x=Math.asin(Ze(h,-1,1)),Math.abs(h)<.9999999?(this._y=Math.atan2(-d,g),this._z=Math.atan2(-a,u)):(this._y=0,this._z=Math.atan2(l,o));break;case"ZYX":this._y=Math.asin(-Ze(d,-1,1)),Math.abs(d)<.9999999?(this._x=Math.atan2(h,g),this._z=Math.atan2(l,o)):(this._x=0,this._z=Math.atan2(-a,u));break;case"YZX":this._z=Math.asin(Ze(l,-1,1)),Math.abs(l)<.9999999?(this._x=Math.atan2(-f,u),this._y=Math.atan2(-d,o)):(this._x=0,this._y=Math.atan2(c,g));break;case"XZY":this._z=Math.asin(-Ze(a,-1,1)),Math.abs(a)<.9999999?(this._x=Math.atan2(h,u),this._y=Math.atan2(c,o)):(this._x=Math.atan2(-f,g),this._y=0);break;default:Ne("Euler: .setFromRotationMatrix() encountered an unknown order: "+i)}return this._order=i,r===!0&&this._onChangeCallback(),this}setFromQuaternion(t,i,r){return jx.makeRotationFromQuaternion(t),this.setFromRotationMatrix(jx,i,r)}setFromVector3(t,i=this._order){return this.set(t.x,t.y,t.z,i)}reorder(t){return $x.setFromEuler(this),this.setFromQuaternion($x,t)}equals(t){return t._x===this._x&&t._y===this._y&&t._z===this._z&&t._order===this._order}fromArray(t){return this._x=t[0],this._y=t[1],this._z=t[2],t[3]!==void 0&&(this._order=t[3]),this._onChangeCallback(),this}toArray(t=[],i=0){return t[i]=this._x,t[i+1]=this._y,t[i+2]=this._z,t[i+3]=this._order,t}_onChange(t){return this._onChangeCallback=t,this}_onChangeCallback(){}*[Symbol.iterator](){yield this._x,yield this._y,yield this._z,yield this._order}}return n.DEFAULT_ORDER="XYZ",n})(),La=class{constructor(){this.mask=1}set(e){this.mask=(1<<e|0)>>>0}enable(e){this.mask|=1<<e|0}enableAll(){this.mask=-1}toggle(e){this.mask^=1<<e|0}disable(e){this.mask&=~(1<<e|0)}disableAll(){this.mask=0}test(e){return(this.mask&e.mask)!==0}isEnabled(e){return(this.mask&(1<<e|0))!==0}},vC=0,qx=new k,uo=new wi,Xi=new Ut,$l=new k,Ea=new k,yC=new k,_C=new wi,Xx=new k(1,0,0),Yx=new k(0,1,0),Zx=new k(0,0,1),Jx={type:"added"},xC={type:"removed"},fo={type:"childadded",child:null},pm={type:"childremoved",child:null},sr=(()=>{class n extends nr{constructor(){super(),this.isObject3D=!0,Object.defineProperty(this,"id",{value:vC++}),this.uuid=nc(),this.name="",this.type="Object3D",this.parent=null,this.children=[],this.up=n.DEFAULT_UP.clone();let t=new k,i=new bs,r=new wi,s=new k(1,1,1);function o(){r.setFromEuler(i,!1)}function a(){i.setFromQuaternion(r,void 0,!1)}i._onChange(o),r._onChange(a),Object.defineProperties(this,{position:{configurable:!0,enumerable:!0,value:t},rotation:{configurable:!0,enumerable:!0,value:i},quaternion:{configurable:!0,enumerable:!0,value:r},scale:{configurable:!0,enumerable:!0,value:s},modelViewMatrix:{value:new Ut},normalMatrix:{value:new Be}}),this.matrix=new Ut,this.matrixWorld=new Ut,this.matrixAutoUpdate=n.DEFAULT_MATRIX_AUTO_UPDATE,this.matrixWorldAutoUpdate=n.DEFAULT_MATRIX_WORLD_AUTO_UPDATE,this.matrixWorldNeedsUpdate=!1,this.layers=new La,this.visible=!0,this.castShadow=!1,this.receiveShadow=!1,this.frustumCulled=!0,this.renderOrder=0,this.animations=[],this.customDepthMaterial=void 0,this.customDistanceMaterial=void 0,this.static=!1,this.userData={},this.pivot=null}onBeforeShadow(){}onAfterShadow(){}onBeforeRender(){}onAfterRender(){}applyMatrix4(t){this.matrixAutoUpdate&&this.updateMatrix(),this.matrix.premultiply(t),this.matrix.decompose(this.position,this.quaternion,this.scale)}applyQuaternion(t){return this.quaternion.premultiply(t),this}setRotationFromAxisAngle(t,i){this.quaternion.setFromAxisAngle(t,i)}setRotationFromEuler(t){this.quaternion.setFromEuler(t,!0)}setRotationFromMatrix(t){this.quaternion.setFromRotationMatrix(t)}setRotationFromQuaternion(t){this.quaternion.copy(t)}rotateOnAxis(t,i){return uo.setFromAxisAngle(t,i),this.quaternion.multiply(uo),this}rotateOnWorldAxis(t,i){return uo.setFromAxisAngle(t,i),this.quaternion.premultiply(uo),this}rotateX(t){return this.rotateOnAxis(Xx,t)}rotateY(t){return this.rotateOnAxis(Yx,t)}rotateZ(t){return this.rotateOnAxis(Zx,t)}translateOnAxis(t,i){return qx.copy(t).applyQuaternion(this.quaternion),this.position.add(qx.multiplyScalar(i)),this}translateX(t){return this.translateOnAxis(Xx,t)}translateY(t){return this.translateOnAxis(Yx,t)}translateZ(t){return this.translateOnAxis(Zx,t)}localToWorld(t){return this.updateWorldMatrix(!0,!1),t.applyMatrix4(this.matrixWorld)}worldToLocal(t){return this.updateWorldMatrix(!0,!1),t.applyMatrix4(Xi.copy(this.matrixWorld).invert())}lookAt(t,i,r){t.isVector3?$l.copy(t):$l.set(t,i,r);let s=this.parent;this.updateWorldMatrix(!0,!1),Ea.setFromMatrixPosition(this.matrixWorld),this.isCamera||this.isLight?Xi.lookAt(Ea,$l,this.up):Xi.lookAt($l,Ea,this.up),this.quaternion.setFromRotationMatrix(Xi),s&&(Xi.extractRotation(s.matrixWorld),uo.setFromRotationMatrix(Xi),this.quaternion.premultiply(uo.invert()))}add(t){if(arguments.length>1){for(let i=0;i<arguments.length;i++)this.add(arguments[i]);return this}return t===this?(Re("Object3D.add: object can't be added as a child of itself.",t),this):(t&&t.isObject3D?(t.removeFromParent(),t.parent=this,this.children.push(t),t.dispatchEvent(Jx),fo.child=t,this.dispatchEvent(fo),fo.child=null):Re("Object3D.add: object not an instance of THREE.Object3D.",t),this)}remove(t){if(arguments.length>1){for(let r=0;r<arguments.length;r++)this.remove(arguments[r]);return this}let i=this.children.indexOf(t);return i!==-1&&(t.parent=null,this.children.splice(i,1),t.dispatchEvent(xC),pm.child=t,this.dispatchEvent(pm),pm.child=null),this}removeFromParent(){let t=this.parent;return t!==null&&t.remove(this),this}clear(){return this.remove(...this.children)}attach(t){return this.updateWorldMatrix(!0,!1),Xi.copy(this.matrixWorld).invert(),t.parent!==null&&(t.parent.updateWorldMatrix(!0,!1),Xi.multiply(t.parent.matrixWorld)),t.applyMatrix4(Xi),t.removeFromParent(),t.parent=this,this.children.push(t),t.updateWorldMatrix(!1,!0),t.dispatchEvent(Jx),fo.child=t,this.dispatchEvent(fo),fo.child=null,this}getObjectById(t){return this.getObjectByProperty("id",t)}getObjectByName(t){return this.getObjectByProperty("name",t)}getObjectByProperty(t,i){if(this[t]===i)return this;for(let r=0,s=this.children.length;r<s;r++){let a=this.children[r].getObjectByProperty(t,i);if(a!==void 0)return a}}getObjectsByProperty(t,i,r=[]){this[t]===i&&r.push(this);let s=this.children;for(let o=0,a=s.length;o<a;o++)s[o].getObjectsByProperty(t,i,r);return r}getWorldPosition(t){return this.updateWorldMatrix(!0,!1),t.setFromMatrixPosition(this.matrixWorld)}getWorldQuaternion(t){return this.updateWorldMatrix(!0,!1),this.matrixWorld.decompose(Ea,t,yC),t}getWorldScale(t){return this.updateWorldMatrix(!0,!1),this.matrixWorld.decompose(Ea,_C,t),t}getWorldDirection(t){this.updateWorldMatrix(!0,!1);let i=this.matrixWorld.elements;return t.set(i[8],i[9],i[10]).normalize()}raycast(){}traverse(t){t(this);let i=this.children;for(let r=0,s=i.length;r<s;r++)i[r].traverse(t)}traverseVisible(t){if(this.visible===!1)return;t(this);let i=this.children;for(let r=0,s=i.length;r<s;r++)i[r].traverseVisible(t)}traverseAncestors(t){let i=this.parent;i!==null&&(t(i),i.traverseAncestors(t))}updateMatrix(){this.matrix.compose(this.position,this.quaternion,this.scale);let t=this.pivot;if(t!==null){let i=t.x,r=t.y,s=t.z,o=this.matrix.elements;o[12]+=i-o[0]*i-o[4]*r-o[8]*s,o[13]+=r-o[1]*i-o[5]*r-o[9]*s,o[14]+=s-o[2]*i-o[6]*r-o[10]*s}this.matrixWorldNeedsUpdate=!0}updateMatrixWorld(t){this.matrixAutoUpdate&&this.updateMatrix(),(this.matrixWorldNeedsUpdate||t)&&(this.matrixWorldAutoUpdate===!0&&(this.parent===null?this.matrixWorld.copy(this.matrix):this.matrixWorld.multiplyMatrices(this.parent.matrixWorld,this.matrix)),this.matrixWorldNeedsUpdate=!1,t=!0);let i=this.children;for(let r=0,s=i.length;r<s;r++)i[r].updateMatrixWorld(t)}updateWorldMatrix(t,i){let r=this.parent;if(t===!0&&r!==null&&r.updateWorldMatrix(!0,!1),this.matrixAutoUpdate&&this.updateMatrix(),this.matrixWorldAutoUpdate===!0&&(this.parent===null?this.matrixWorld.copy(this.matrix):this.matrixWorld.multiplyMatrices(this.parent.matrixWorld,this.matrix)),i===!0){let s=this.children;for(let o=0,a=s.length;o<a;o++)s[o].updateWorldMatrix(!1,!0)}}toJSON(t){let i=t===void 0||typeof t=="string",r={};i&&(t={geometries:{},materials:{},textures:{},images:{},shapes:{},skeletons:{},animations:{},nodes:{}},r.metadata={version:4.7,type:"Object",generator:"Object3D.toJSON"});let s={};s.uuid=this.uuid,s.type=this.type,this.name!==""&&(s.name=this.name),this.castShadow===!0&&(s.castShadow=!0),this.receiveShadow===!0&&(s.receiveShadow=!0),this.visible===!1&&(s.visible=!1),this.frustumCulled===!1&&(s.frustumCulled=!1),this.renderOrder!==0&&(s.renderOrder=this.renderOrder),this.static!==!1&&(s.static=this.static),Object.keys(this.userData).length>0&&(s.userData=this.userData),s.layers=this.layers.mask,s.matrix=this.matrix.toArray(),s.up=this.up.toArray(),this.pivot!==null&&(s.pivot=this.pivot.toArray()),this.matrixAutoUpdate===!1&&(s.matrixAutoUpdate=!1),this.morphTargetDictionary!==void 0&&(s.morphTargetDictionary=Object.assign({},this.morphTargetDictionary)),this.morphTargetInfluences!==void 0&&(s.morphTargetInfluences=this.morphTargetInfluences.slice()),this.isInstancedMesh&&(s.type="InstancedMesh",s.count=this.count,s.instanceMatrix=this.instanceMatrix.toJSON(),this.instanceColor!==null&&(s.instanceColor=this.instanceColor.toJSON())),this.isBatchedMesh&&(s.type="BatchedMesh",s.perObjectFrustumCulled=this.perObjectFrustumCulled,s.sortObjects=this.sortObjects,s.drawRanges=this._drawRanges,s.reservedRanges=this._reservedRanges,s.geometryInfo=this._geometryInfo.map(c=>Gt(It({},c),{boundingBox:c.boundingBox?c.boundingBox.toJSON():void 0,boundingSphere:c.boundingSphere?c.boundingSphere.toJSON():void 0})),s.instanceInfo=this._instanceInfo.map(c=>It({},c)),s.availableInstanceIds=this._availableInstanceIds.slice(),s.availableGeometryIds=this._availableGeometryIds.slice(),s.nextIndexStart=this._nextIndexStart,s.nextVertexStart=this._nextVertexStart,s.geometryCount=this._geometryCount,s.maxInstanceCount=this._maxInstanceCount,s.maxVertexCount=this._maxVertexCount,s.maxIndexCount=this._maxIndexCount,s.geometryInitialized=this._geometryInitialized,s.matricesTexture=this._matricesTexture.toJSON(t),s.indirectTexture=this._indirectTexture.toJSON(t),this._colorsTexture!==null&&(s.colorsTexture=this._colorsTexture.toJSON(t)),this.boundingSphere!==null&&(s.boundingSphere=this.boundingSphere.toJSON()),this.boundingBox!==null&&(s.boundingBox=this.boundingBox.toJSON()));function o(c,l){return c[l.uuid]===void 0&&(c[l.uuid]=l.toJSON(t)),l.uuid}if(this.isScene)this.background&&(this.background.isColor?s.background=this.background.toJSON():this.background.isTexture&&(s.background=this.background.toJSON(t).uuid)),this.environment&&this.environment.isTexture&&this.environment.isRenderTargetTexture!==!0&&(s.environment=this.environment.toJSON(t).uuid);else if(this.isMesh||this.isLine||this.isPoints){s.geometry=o(t.geometries,this.geometry);let c=this.geometry.parameters;if(c!==void 0&&c.shapes!==void 0){let l=c.shapes;if(Array.isArray(l))for(let u=0,f=l.length;u<f;u++){let d=l[u];o(t.shapes,d)}else o(t.shapes,l)}}if(this.isSkinnedMesh&&(s.bindMode=this.bindMode,s.bindMatrix=this.bindMatrix.toArray(),this.skeleton!==void 0&&(o(t.skeletons,this.skeleton),s.skeleton=this.skeleton.uuid)),this.material!==void 0)if(Array.isArray(this.material)){let c=[];for(let l=0,u=this.material.length;l<u;l++)c.push(o(t.materials,this.material[l]));s.material=c}else s.material=o(t.materials,this.material);if(this.children.length>0){s.children=[];for(let c=0;c<this.children.length;c++)s.children.push(this.children[c].toJSON(t).object)}if(this.animations.length>0){s.animations=[];for(let c=0;c<this.animations.length;c++){let l=this.animations[c];s.animations.push(o(t.animations,l))}}if(i){let c=a(t.geometries),l=a(t.materials),u=a(t.textures),f=a(t.images),d=a(t.shapes),h=a(t.skeletons),g=a(t.animations),x=a(t.nodes);c.length>0&&(r.geometries=c),l.length>0&&(r.materials=l),u.length>0&&(r.textures=u),f.length>0&&(r.images=f),d.length>0&&(r.shapes=d),h.length>0&&(r.skeletons=h),g.length>0&&(r.animations=g),x.length>0&&(r.nodes=x)}return r.object=s,r;function a(c){let l=[];for(let u in c){let f=c[u];delete f.metadata,l.push(f)}return l}}clone(t){return new this.constructor().copy(this,t)}copy(t,i=!0){if(this.name=t.name,this.up.copy(t.up),this.position.copy(t.position),this.rotation.order=t.rotation.order,this.quaternion.copy(t.quaternion),this.scale.copy(t.scale),t.pivot!==null&&(this.pivot=t.pivot.clone()),this.matrix.copy(t.matrix),this.matrixWorld.copy(t.matrixWorld),this.matrixAutoUpdate=t.matrixAutoUpdate,this.matrixWorldAutoUpdate=t.matrixWorldAutoUpdate,this.matrixWorldNeedsUpdate=t.matrixWorldNeedsUpdate,this.layers.mask=t.layers.mask,this.visible=t.visible,this.castShadow=t.castShadow,this.receiveShadow=t.receiveShadow,this.frustumCulled=t.frustumCulled,this.renderOrder=t.renderOrder,this.static=t.static,this.animations=t.animations.slice(),this.userData=JSON.parse(JSON.stringify(t.userData)),i===!0)for(let r=0;r<t.children.length;r++){let s=t.children[r];this.add(s.clone())}return this}}return n.DEFAULT_UP=new k(0,1,0),n.DEFAULT_MATRIX_AUTO_UPDATE=!0,n.DEFAULT_MATRIX_WORLD_AUTO_UPDATE=!0,n})(),_s=class extends sr{constructor(){super(),this.isGroup=!0,this.type="Group"}},MC={type:"move"},Eo=class{constructor(){this._targetRay=null,this._grip=null,this._hand=null}getHandSpace(){return this._hand===null&&(this._hand=new _s,this._hand.matrixAutoUpdate=!1,this._hand.visible=!1,this._hand.joints={},this._hand.inputState={pinching:!1}),this._hand}getTargetRaySpace(){return this._targetRay===null&&(this._targetRay=new _s,this._targetRay.matrixAutoUpdate=!1,this._targetRay.visible=!1,this._targetRay.hasLinearVelocity=!1,this._targetRay.linearVelocity=new k,this._targetRay.hasAngularVelocity=!1,this._targetRay.angularVelocity=new k),this._targetRay}getGripSpace(){return this._grip===null&&(this._grip=new _s,this._grip.matrixAutoUpdate=!1,this._grip.visible=!1,this._grip.hasLinearVelocity=!1,this._grip.linearVelocity=new k,this._grip.hasAngularVelocity=!1,this._grip.angularVelocity=new k),this._grip}dispatchEvent(e){return this._targetRay!==null&&this._targetRay.dispatchEvent(e),this._grip!==null&&this._grip.dispatchEvent(e),this._hand!==null&&this._hand.dispatchEvent(e),this}connect(e){if(e&&e.hand){let t=this._hand;if(t)for(let i of e.hand.values())this._getHandJoint(t,i)}return this.dispatchEvent({type:"connected",data:e}),this}disconnect(e){return this.dispatchEvent({type:"disconnected",data:e}),this._targetRay!==null&&(this._targetRay.visible=!1),this._grip!==null&&(this._grip.visible=!1),this._hand!==null&&(this._hand.visible=!1),this}update(e,t,i){let r=null,s=null,o=null,a=this._targetRay,c=this._grip,l=this._hand;if(e&&t.session.visibilityState!=="visible-blurred"){if(l&&e.hand){o=!0;for(let x of e.hand.values()){let m=t.getJointPose(x,i),p=this._getHandJoint(l,x);m!==null&&(p.matrix.fromArray(m.transform.matrix),p.matrix.decompose(p.position,p.rotation,p.scale),p.matrixWorldNeedsUpdate=!0,p.jointRadius=m.radius),p.visible=m!==null}let u=l.joints["index-finger-tip"],f=l.joints["thumb-tip"],d=u.position.distanceTo(f.position),h=.02,g=.005;l.inputState.pinching&&d>h+g?(l.inputState.pinching=!1,this.dispatchEvent({type:"pinchend",handedness:e.handedness,target:this})):!l.inputState.pinching&&d<=h-g&&(l.inputState.pinching=!0,this.dispatchEvent({type:"pinchstart",handedness:e.handedness,target:this}))}else c!==null&&e.gripSpace&&(s=t.getPose(e.gripSpace,i),s!==null&&(c.matrix.fromArray(s.transform.matrix),c.matrix.decompose(c.position,c.rotation,c.scale),c.matrixWorldNeedsUpdate=!0,s.linearVelocity?(c.hasLinearVelocity=!0,c.linearVelocity.copy(s.linearVelocity)):c.hasLinearVelocity=!1,s.angularVelocity?(c.hasAngularVelocity=!0,c.angularVelocity.copy(s.angularVelocity)):c.hasAngularVelocity=!1));a!==null&&(r=t.getPose(e.targetRaySpace,i),r===null&&s!==null&&(r=s),r!==null&&(a.matrix.fromArray(r.transform.matrix),a.matrix.decompose(a.position,a.rotation,a.scale),a.matrixWorldNeedsUpdate=!0,r.linearVelocity?(a.hasLinearVelocity=!0,a.linearVelocity.copy(r.linearVelocity)):a.hasLinearVelocity=!1,r.angularVelocity?(a.hasAngularVelocity=!0,a.angularVelocity.copy(r.angularVelocity)):a.hasAngularVelocity=!1,this.dispatchEvent(MC)))}return a!==null&&(a.visible=r!==null),c!==null&&(c.visible=s!==null),l!==null&&(l.visible=o!==null),this}_getHandJoint(e,t){if(e.joints[t.jointName]===void 0){let i=new _s;i.matrixAutoUpdate=!1,i.visible=!1,e.joints[t.jointName]=i,e.add(i)}return e.joints[t.jointName]}},G0={aliceblue:15792383,antiquewhite:16444375,aqua:65535,aquamarine:8388564,azure:15794175,beige:16119260,bisque:16770244,black:0,blanchedalmond:16772045,blue:255,blueviolet:9055202,brown:10824234,burlywood:14596231,cadetblue:6266528,chartreuse:8388352,chocolate:13789470,coral:16744272,cornflowerblue:6591981,cornsilk:16775388,crimson:14423100,cyan:65535,darkblue:139,darkcyan:35723,darkgoldenrod:12092939,darkgray:11119017,darkgreen:25600,darkgrey:11119017,darkkhaki:12433259,darkmagenta:9109643,darkolivegreen:5597999,darkorange:16747520,darkorchid:10040012,darkred:9109504,darksalmon:15308410,darkseagreen:9419919,darkslateblue:4734347,darkslategray:3100495,darkslategrey:3100495,darkturquoise:52945,darkviolet:9699539,deeppink:16716947,deepskyblue:49151,dimgray:6908265,dimgrey:6908265,dodgerblue:2003199,firebrick:11674146,floralwhite:16775920,forestgreen:2263842,fuchsia:16711935,gainsboro:14474460,ghostwhite:16316671,gold:16766720,goldenrod:14329120,gray:8421504,green:32768,greenyellow:11403055,grey:8421504,honeydew:15794160,hotpink:16738740,indianred:13458524,indigo:4915330,ivory:16777200,khaki:15787660,lavender:15132410,lavenderblush:16773365,lawngreen:8190976,lemonchiffon:16775885,lightblue:11393254,lightcoral:15761536,lightcyan:14745599,lightgoldenrodyellow:16448210,lightgray:13882323,lightgreen:9498256,lightgrey:13882323,lightpink:16758465,lightsalmon:16752762,lightseagreen:2142890,lightskyblue:8900346,lightslategray:7833753,lightslategrey:7833753,lightsteelblue:11584734,lightyellow:16777184,lime:65280,limegreen:3329330,linen:16445670,magenta:16711935,maroon:8388608,mediumaquamarine:6737322,mediumblue:205,mediumorchid:12211667,mediumpurple:9662683,mediumseagreen:3978097,mediumslateblue:8087790,mediumspringgreen:64154,mediumturquoise:4772300,mediumvioletred:13047173,midnightblue:1644912,mintcream:16121850,mistyrose:16770273,moccasin:16770229,navajowhite:16768685,navy:128,oldlace:16643558,olive:8421376,olivedrab:7048739,orange:16753920,orangered:16729344,orchid:14315734,palegoldenrod:15657130,palegreen:10025880,paleturquoise:11529966,palevioletred:14381203,papayawhip:16773077,peachpuff:16767673,peru:13468991,pink:16761035,plum:14524637,powderblue:11591910,purple:8388736,rebeccapurple:6697881,red:16711680,rosybrown:12357519,royalblue:4286945,saddlebrown:9127187,salmon:16416882,sandybrown:16032864,seagreen:3050327,seashell:16774638,sienna:10506797,silver:12632256,skyblue:8900331,slateblue:6970061,slategray:7372944,slategrey:7372944,snow:16775930,springgreen:65407,steelblue:4620980,tan:13808780,teal:32896,thistle:14204888,tomato:16737095,turquoise:4251856,violet:15631086,wheat:16113331,white:16777215,whitesmoke:16119285,yellow:16776960,yellowgreen:10145074},br={h:0,s:0,l:0},ql={h:0,s:0,l:0};function mm(n,e,t){return t<0&&(t+=1),t>1&&(t-=1),t<1/6?n+(e-n)*6*t:t<1/2?e:t<2/3?n+(e-n)*6*(2/3-t):n}var Xe=class{constructor(e,t,i){return this.isColor=!0,this.r=1,this.g=1,this.b=1,this.set(e,t,i)}set(e,t,i){if(t===void 0&&i===void 0){let r=e;r&&r.isColor?this.copy(r):typeof r=="number"?this.setHex(r):typeof r=="string"&&this.setStyle(r)}else this.setRGB(e,t,i);return this}setScalar(e){return this.r=e,this.g=e,this.b=e,this}setHex(e,t=Dn){return e=Math.floor(e),this.r=(e>>16&255)/255,this.g=(e>>8&255)/255,this.b=(e&255)/255,tt.colorSpaceToWorking(this,t),this}setRGB(e,t,i,r=tt.workingColorSpace){return this.r=e,this.g=t,this.b=i,tt.colorSpaceToWorking(this,r),this}setHSL(e,t,i,r=tt.workingColorSpace){if(e=dC(e,1),t=Ze(t,0,1),i=Ze(i,0,1),t===0)this.r=this.g=this.b=i;else{let s=i<=.5?i*(1+t):i+t-i*t,o=2*i-s;this.r=mm(o,s,e+1/3),this.g=mm(o,s,e),this.b=mm(o,s,e-1/3)}return tt.colorSpaceToWorking(this,r),this}setStyle(e,t=Dn){function i(s){s!==void 0&&parseFloat(s)<1&&Ne("Color: Alpha component of "+e+" will be ignored.")}let r;if(r=/^(\w+)\(([^\)]*)\)/.exec(e)){let s,o=r[1],a=r[2];switch(o){case"rgb":case"rgba":if(s=/^\s*(\d+)\s*,\s*(\d+)\s*,\s*(\d+)\s*(?:,\s*(\d*\.?\d+)\s*)?$/.exec(a))return i(s[4]),this.setRGB(Math.min(255,parseInt(s[1],10))/255,Math.min(255,parseInt(s[2],10))/255,Math.min(255,parseInt(s[3],10))/255,t);if(s=/^\s*(\d+)\%\s*,\s*(\d+)\%\s*,\s*(\d+)\%\s*(?:,\s*(\d*\.?\d+)\s*)?$/.exec(a))return i(s[4]),this.setRGB(Math.min(100,parseInt(s[1],10))/100,Math.min(100,parseInt(s[2],10))/100,Math.min(100,parseInt(s[3],10))/100,t);break;case"hsl":case"hsla":if(s=/^\s*(\d*\.?\d+)\s*,\s*(\d*\.?\d+)\%\s*,\s*(\d*\.?\d+)\%\s*(?:,\s*(\d*\.?\d+)\s*)?$/.exec(a))return i(s[4]),this.setHSL(parseFloat(s[1])/360,parseFloat(s[2])/100,parseFloat(s[3])/100,t);break;default:Ne("Color: Unknown color model "+e)}}else if(r=/^\#([A-Fa-f\d]+)$/.exec(e)){let s=r[1],o=s.length;if(o===3)return this.setRGB(parseInt(s.charAt(0),16)/15,parseInt(s.charAt(1),16)/15,parseInt(s.charAt(2),16)/15,t);if(o===6)return this.setHex(parseInt(s,16),t);Ne("Color: Invalid hex color "+e)}else if(e&&e.length>0)return this.setColorName(e,t);return this}setColorName(e,t=Dn){let i=G0[e.toLowerCase()];return i!==void 0?this.setHex(i,t):Ne("Color: Unknown color "+e),this}clone(){return new this.constructor(this.r,this.g,this.b)}copy(e){return this.r=e.r,this.g=e.g,this.b=e.b,this}copySRGBToLinear(e){return this.r=Qi(e.r),this.g=Qi(e.g),this.b=Qi(e.b),this}copyLinearToSRGB(e){return this.r=Mo(e.r),this.g=Mo(e.g),this.b=Mo(e.b),this}convertSRGBToLinear(){return this.copySRGBToLinear(this),this}convertLinearToSRGB(){return this.copyLinearToSRGB(this),this}getHex(e=Dn){return tt.workingToColorSpace(dn.copy(this),e),Math.round(Ze(dn.r*255,0,255))*65536+Math.round(Ze(dn.g*255,0,255))*256+Math.round(Ze(dn.b*255,0,255))}getHexString(e=Dn){return("000000"+this.getHex(e).toString(16)).slice(-6)}getHSL(e,t=tt.workingColorSpace){tt.workingToColorSpace(dn.copy(this),t);let i=dn.r,r=dn.g,s=dn.b,o=Math.max(i,r,s),a=Math.min(i,r,s),c,l,u=(a+o)/2;if(a===o)c=0,l=0;else{let f=o-a;switch(l=u<=.5?f/(o+a):f/(2-o-a),o){case i:c=(r-s)/f+(r<s?6:0);break;case r:c=(s-i)/f+2;break;case s:c=(i-r)/f+4;break}c/=6}return e.h=c,e.s=l,e.l=u,e}getRGB(e,t=tt.workingColorSpace){return tt.workingToColorSpace(dn.copy(this),t),e.r=dn.r,e.g=dn.g,e.b=dn.b,e}getStyle(e=Dn){tt.workingToColorSpace(dn.copy(this),e);let t=dn.r,i=dn.g,r=dn.b;return e!==Dn?`color(${e} ${t.toFixed(3)} ${i.toFixed(3)} ${r.toFixed(3)})`:`rgb(${Math.round(t*255)},${Math.round(i*255)},${Math.round(r*255)})`}offsetHSL(e,t,i){return this.getHSL(br),this.setHSL(br.h+e,br.s+t,br.l+i)}add(e){return this.r+=e.r,this.g+=e.g,this.b+=e.b,this}addColors(e,t){return this.r=e.r+t.r,this.g=e.g+t.g,this.b=e.b+t.b,this}addScalar(e){return this.r+=e,this.g+=e,this.b+=e,this}sub(e){return this.r=Math.max(0,this.r-e.r),this.g=Math.max(0,this.g-e.g),this.b=Math.max(0,this.b-e.b),this}multiply(e){return this.r*=e.r,this.g*=e.g,this.b*=e.b,this}multiplyScalar(e){return this.r*=e,this.g*=e,this.b*=e,this}lerp(e,t){return this.r+=(e.r-this.r)*t,this.g+=(e.g-this.g)*t,this.b+=(e.b-this.b)*t,this}lerpColors(e,t,i){return this.r=e.r+(t.r-e.r)*i,this.g=e.g+(t.g-e.g)*i,this.b=e.b+(t.b-e.b)*i,this}lerpHSL(e,t){this.getHSL(br),e.getHSL(ql);let i=lm(br.h,ql.h,t),r=lm(br.s,ql.s,t),s=lm(br.l,ql.l,t);return this.setHSL(i,r,s),this}setFromVector3(e){return this.r=e.x,this.g=e.y,this.b=e.z,this}applyMatrix3(e){let t=this.r,i=this.g,r=this.b,s=e.elements;return this.r=s[0]*t+s[3]*i+s[6]*r,this.g=s[1]*t+s[4]*i+s[7]*r,this.b=s[2]*t+s[5]*i+s[8]*r,this}equals(e){return e.r===this.r&&e.g===this.g&&e.b===this.b}fromArray(e,t=0){return this.r=e[t],this.g=e[t+1],this.b=e[t+2],this}toArray(e=[],t=0){return e[t]=this.r,e[t+1]=this.g,e[t+2]=this.b,e}fromBufferAttribute(e,t){return this.r=e.getX(t),this.g=e.getY(t),this.b=e.getZ(t),this}toJSON(){return this.getHex()}*[Symbol.iterator](){yield this.r,yield this.g,yield this.b}},dn=new Xe;Xe.NAMES=G0;var Oa=class extends sr{constructor(){super(),this.isScene=!0,this.type="Scene",this.background=null,this.environment=null,this.fog=null,this.backgroundBlurriness=0,this.backgroundIntensity=1,this.backgroundRotation=new bs,this.environmentIntensity=1,this.environmentRotation=new bs,this.overrideMaterial=null,typeof __THREE_DEVTOOLS__<"u"&&__THREE_DEVTOOLS__.dispatchEvent(new CustomEvent("observe",{detail:this}))}copy(e,t){return super.copy(e,t),e.background!==null&&(this.background=e.background.clone()),e.environment!==null&&(this.environment=e.environment.clone()),e.fog!==null&&(this.fog=e.fog.clone()),this.backgroundBlurriness=e.backgroundBlurriness,this.backgroundIntensity=e.backgroundIntensity,this.backgroundRotation.copy(e.backgroundRotation),this.environmentIntensity=e.environmentIntensity,this.environmentRotation.copy(e.environmentRotation),e.overrideMaterial!==null&&(this.overrideMaterial=e.overrideMaterial.clone()),this.matrixAutoUpdate=e.matrixAutoUpdate,this}toJSON(e){let t=super.toJSON(e);return this.fog!==null&&(t.object.fog=this.fog.toJSON()),this.backgroundBlurriness>0&&(t.object.backgroundBlurriness=this.backgroundBlurriness),this.backgroundIntensity!==1&&(t.object.backgroundIntensity=this.backgroundIntensity),t.object.backgroundRotation=this.backgroundRotation.toArray(),this.environmentIntensity!==1&&(t.object.environmentIntensity=this.environmentIntensity),t.object.environmentRotation=this.environmentRotation.toArray(),t}},ci=new k,Yi=new k,gm=new k,Zi=new k,ho=new k,po=new k,Kx=new k,vm=new k,ym=new k,_m=new k,xm=new Pt,Mm=new Pt,bm=new Pt,Cr=class n{constructor(e=new k,t=new k,i=new k){this.a=e,this.b=t,this.c=i}static getNormal(e,t,i,r){r.subVectors(i,t),ci.subVectors(e,t),r.cross(ci);let s=r.lengthSq();return s>0?r.multiplyScalar(1/Math.sqrt(s)):r.set(0,0,0)}static getBarycoord(e,t,i,r,s){ci.subVectors(r,t),Yi.subVectors(i,t),gm.subVectors(e,t);let o=ci.dot(ci),a=ci.dot(Yi),c=ci.dot(gm),l=Yi.dot(Yi),u=Yi.dot(gm),f=o*l-a*a;if(f===0)return s.set(0,0,0),null;let d=1/f,h=(l*c-a*u)*d,g=(o*u-a*c)*d;return s.set(1-h-g,g,h)}static containsPoint(e,t,i,r){return this.getBarycoord(e,t,i,r,Zi)===null?!1:Zi.x>=0&&Zi.y>=0&&Zi.x+Zi.y<=1}static getInterpolation(e,t,i,r,s,o,a,c){return this.getBarycoord(e,t,i,r,Zi)===null?(c.x=0,c.y=0,"z"in c&&(c.z=0),"w"in c&&(c.w=0),null):(c.setScalar(0),c.addScaledVector(s,Zi.x),c.addScaledVector(o,Zi.y),c.addScaledVector(a,Zi.z),c)}static getInterpolatedAttribute(e,t,i,r,s,o){return xm.setScalar(0),Mm.setScalar(0),bm.setScalar(0),xm.fromBufferAttribute(e,t),Mm.fromBufferAttribute(e,i),bm.fromBufferAttribute(e,r),o.setScalar(0),o.addScaledVector(xm,s.x),o.addScaledVector(Mm,s.y),o.addScaledVector(bm,s.z),o}static isFrontFacing(e,t,i,r){return ci.subVectors(i,t),Yi.subVectors(e,t),ci.cross(Yi).dot(r)<0}set(e,t,i){return this.a.copy(e),this.b.copy(t),this.c.copy(i),this}setFromPointsAndIndices(e,t,i,r){return this.a.copy(e[t]),this.b.copy(e[i]),this.c.copy(e[r]),this}setFromAttributeAndIndices(e,t,i,r){return this.a.fromBufferAttribute(e,t),this.b.fromBufferAttribute(e,i),this.c.fromBufferAttribute(e,r),this}clone(){return new this.constructor().copy(this)}copy(e){return this.a.copy(e.a),this.b.copy(e.b),this.c.copy(e.c),this}getArea(){return ci.subVectors(this.c,this.b),Yi.subVectors(this.a,this.b),ci.cross(Yi).length()*.5}getMidpoint(e){return e.addVectors(this.a,this.b).add(this.c).multiplyScalar(1/3)}getNormal(e){return n.getNormal(this.a,this.b,this.c,e)}getPlane(e){return e.setFromCoplanarPoints(this.a,this.b,this.c)}getBarycoord(e,t){return n.getBarycoord(e,this.a,this.b,this.c,t)}getInterpolation(e,t,i,r,s){return n.getInterpolation(e,this.a,this.b,this.c,t,i,r,s)}containsPoint(e){return n.containsPoint(e,this.a,this.b,this.c)}isFrontFacing(e){return n.isFrontFacing(this.a,this.b,this.c,e)}intersectsBox(e){return e.intersectsTriangle(this)}closestPointToPoint(e,t){let i=this.a,r=this.b,s=this.c,o,a;ho.subVectors(r,i),po.subVectors(s,i),vm.subVectors(e,i);let c=ho.dot(vm),l=po.dot(vm);if(c<=0&&l<=0)return t.copy(i);ym.subVectors(e,r);let u=ho.dot(ym),f=po.dot(ym);if(u>=0&&f<=u)return t.copy(r);let d=c*f-u*l;if(d<=0&&c>=0&&u<=0)return o=c/(c-u),t.copy(i).addScaledVector(ho,o);_m.subVectors(e,s);let h=ho.dot(_m),g=po.dot(_m);if(g>=0&&h<=g)return t.copy(s);let x=h*l-c*g;if(x<=0&&l>=0&&g<=0)return a=l/(l-g),t.copy(i).addScaledVector(po,a);let m=u*g-h*f;if(m<=0&&f-u>=0&&h-g>=0)return Kx.subVectors(s,r),a=(f-u)/(f-u+(h-g)),t.copy(r).addScaledVector(Kx,a);let p=1/(m+x+d);return o=x*p,a=d*p,t.copy(i).addScaledVector(ho,o).addScaledVector(po,a)}equals(e){return e.a.equals(this.a)&&e.b.equals(this.b)&&e.c.equals(this.c)}},Ar=class{constructor(e=new k(1/0,1/0,1/0),t=new k(-1/0,-1/0,-1/0)){this.isBox3=!0,this.min=e,this.max=t}set(e,t){return this.min.copy(e),this.max.copy(t),this}setFromArray(e){this.makeEmpty();for(let t=0,i=e.length;t<i;t+=3)this.expandByPoint(li.fromArray(e,t));return this}setFromBufferAttribute(e){this.makeEmpty();for(let t=0,i=e.count;t<i;t++)this.expandByPoint(li.fromBufferAttribute(e,t));return this}setFromPoints(e){this.makeEmpty();for(let t=0,i=e.length;t<i;t++)this.expandByPoint(e[t]);return this}setFromCenterAndSize(e,t){let i=li.copy(t).multiplyScalar(.5);return this.min.copy(e).sub(i),this.max.copy(e).add(i),this}setFromObject(e,t=!1){return this.makeEmpty(),this.expandByObject(e,t)}clone(){return new this.constructor().copy(this)}copy(e){return this.min.copy(e.min),this.max.copy(e.max),this}makeEmpty(){return this.min.x=this.min.y=this.min.z=1/0,this.max.x=this.max.y=this.max.z=-1/0,this}isEmpty(){return this.max.x<this.min.x||this.max.y<this.min.y||this.max.z<this.min.z}getCenter(e){return this.isEmpty()?e.set(0,0,0):e.addVectors(this.min,this.max).multiplyScalar(.5)}getSize(e){return this.isEmpty()?e.set(0,0,0):e.subVectors(this.max,this.min)}expandByPoint(e){return this.min.min(e),this.max.max(e),this}expandByVector(e){return this.min.sub(e),this.max.add(e),this}expandByScalar(e){return this.min.addScalar(-e),this.max.addScalar(e),this}expandByObject(e,t=!1){e.updateWorldMatrix(!1,!1);let i=e.geometry;if(i!==void 0){let s=i.getAttribute("position");if(t===!0&&s!==void 0&&e.isInstancedMesh!==!0)for(let o=0,a=s.count;o<a;o++)e.isMesh===!0?e.getVertexPosition(o,li):li.fromBufferAttribute(s,o),li.applyMatrix4(e.matrixWorld),this.expandByPoint(li);else e.boundingBox!==void 0?(e.boundingBox===null&&e.computeBoundingBox(),Xl.copy(e.boundingBox)):(i.boundingBox===null&&i.computeBoundingBox(),Xl.copy(i.boundingBox)),Xl.applyMatrix4(e.matrixWorld),this.union(Xl)}let r=e.children;for(let s=0,o=r.length;s<o;s++)this.expandByObject(r[s],t);return this}containsPoint(e){return e.x>=this.min.x&&e.x<=this.max.x&&e.y>=this.min.y&&e.y<=this.max.y&&e.z>=this.min.z&&e.z<=this.max.z}containsBox(e){return this.min.x<=e.min.x&&e.max.x<=this.max.x&&this.min.y<=e.min.y&&e.max.y<=this.max.y&&this.min.z<=e.min.z&&e.max.z<=this.max.z}getParameter(e,t){return t.set((e.x-this.min.x)/(this.max.x-this.min.x),(e.y-this.min.y)/(this.max.y-this.min.y),(e.z-this.min.z)/(this.max.z-this.min.z))}intersectsBox(e){return e.max.x>=this.min.x&&e.min.x<=this.max.x&&e.max.y>=this.min.y&&e.min.y<=this.max.y&&e.max.z>=this.min.z&&e.min.z<=this.max.z}intersectsSphere(e){return this.clampPoint(e.center,li),li.distanceToSquared(e.center)<=e.radius*e.radius}intersectsPlane(e){let t,i;return e.normal.x>0?(t=e.normal.x*this.min.x,i=e.normal.x*this.max.x):(t=e.normal.x*this.max.x,i=e.normal.x*this.min.x),e.normal.y>0?(t+=e.normal.y*this.min.y,i+=e.normal.y*this.max.y):(t+=e.normal.y*this.max.y,i+=e.normal.y*this.min.y),e.normal.z>0?(t+=e.normal.z*this.min.z,i+=e.normal.z*this.max.z):(t+=e.normal.z*this.max.z,i+=e.normal.z*this.min.z),t<=-e.constant&&i>=-e.constant}intersectsTriangle(e){if(this.isEmpty())return!1;this.getCenter(Ta),Yl.subVectors(this.max,Ta),mo.subVectors(e.a,Ta),go.subVectors(e.b,Ta),vo.subVectors(e.c,Ta),Sr.subVectors(go,mo),Er.subVectors(vo,go),ps.subVectors(mo,vo);let t=[0,-Sr.z,Sr.y,0,-Er.z,Er.y,0,-ps.z,ps.y,Sr.z,0,-Sr.x,Er.z,0,-Er.x,ps.z,0,-ps.x,-Sr.y,Sr.x,0,-Er.y,Er.x,0,-ps.y,ps.x,0];return!Sm(t,mo,go,vo,Yl)||(t=[1,0,0,0,1,0,0,0,1],!Sm(t,mo,go,vo,Yl))?!1:(Zl.crossVectors(Sr,Er),t=[Zl.x,Zl.y,Zl.z],Sm(t,mo,go,vo,Yl))}clampPoint(e,t){return t.copy(e).clamp(this.min,this.max)}distanceToPoint(e){return this.clampPoint(e,li).distanceTo(e)}getBoundingSphere(e){return this.isEmpty()?e.makeEmpty():(this.getCenter(e.center),e.radius=this.getSize(li).length()*.5),e}intersect(e){return this.min.max(e.min),this.max.min(e.max),this.isEmpty()&&this.makeEmpty(),this}union(e){return this.min.min(e.min),this.max.max(e.max),this}applyMatrix4(e){return this.isEmpty()?this:(Ji[0].set(this.min.x,this.min.y,this.min.z).applyMatrix4(e),Ji[1].set(this.min.x,this.min.y,this.max.z).applyMatrix4(e),Ji[2].set(this.min.x,this.max.y,this.min.z).applyMatrix4(e),Ji[3].set(this.min.x,this.max.y,this.max.z).applyMatrix4(e),Ji[4].set(this.max.x,this.min.y,this.min.z).applyMatrix4(e),Ji[5].set(this.max.x,this.min.y,this.max.z).applyMatrix4(e),Ji[6].set(this.max.x,this.max.y,this.min.z).applyMatrix4(e),Ji[7].set(this.max.x,this.max.y,this.max.z).applyMatrix4(e),this.setFromPoints(Ji),this)}translate(e){return this.min.add(e),this.max.add(e),this}equals(e){return e.min.equals(this.min)&&e.max.equals(this.max)}toJSON(){return{min:this.min.toArray(),max:this.max.toArray()}}fromJSON(e){return this.min.fromArray(e.min),this.max.fromArray(e.max),this}},Ji=[new k,new k,new k,new k,new k,new k,new k,new k],li=new k,Xl=new Ar,mo=new k,go=new k,vo=new k,Sr=new k,Er=new k,ps=new k,Ta=new k,Yl=new k,Zl=new k,ms=new k;function Sm(n,e,t,i,r){for(let s=0,o=n.length-3;s<=o;s+=3){ms.fromArray(n,s);let a=r.x*Math.abs(ms.x)+r.y*Math.abs(ms.y)+r.z*Math.abs(ms.z),c=e.dot(ms),l=t.dot(ms),u=i.dot(ms);if(Math.max(-Math.max(c,l,u),Math.min(c,l,u))>a)return!1}return!0}var zt=new k,Jl=new ht,bC=0,hn=class{constructor(e,t,i=!1){if(Array.isArray(e))throw new TypeError("THREE.BufferAttribute: array should be a Typed Array.");this.isBufferAttribute=!0,Object.defineProperty(this,"id",{value:bC++}),this.name="",this.array=e,this.itemSize=t,this.count=e!==void 0?e.length/t:0,this.normalized=i,this.usage=km,this.updateRanges=[],this.gpuType=hi,this.version=0}onUploadCallback(){}set needsUpdate(e){e===!0&&this.version++}setUsage(e){return this.usage=e,this}addUpdateRange(e,t){this.updateRanges.push({start:e,count:t})}clearUpdateRanges(){this.updateRanges.length=0}copy(e){return this.name=e.name,this.array=new e.array.constructor(e.array),this.itemSize=e.itemSize,this.count=e.count,this.normalized=e.normalized,this.usage=e.usage,this.gpuType=e.gpuType,this}copyAt(e,t,i){e*=this.itemSize,i*=t.itemSize;for(let r=0,s=this.itemSize;r<s;r++)this.array[e+r]=t.array[i+r];return this}copyArray(e){return this.array.set(e),this}applyMatrix3(e){if(this.itemSize===2)for(let t=0,i=this.count;t<i;t++)Jl.fromBufferAttribute(this,t),Jl.applyMatrix3(e),this.setXY(t,Jl.x,Jl.y);else if(this.itemSize===3)for(let t=0,i=this.count;t<i;t++)zt.fromBufferAttribute(this,t),zt.applyMatrix3(e),this.setXYZ(t,zt.x,zt.y,zt.z);return this}applyMatrix4(e){for(let t=0,i=this.count;t<i;t++)zt.fromBufferAttribute(this,t),zt.applyMatrix4(e),this.setXYZ(t,zt.x,zt.y,zt.z);return this}applyNormalMatrix(e){for(let t=0,i=this.count;t<i;t++)zt.fromBufferAttribute(this,t),zt.applyNormalMatrix(e),this.setXYZ(t,zt.x,zt.y,zt.z);return this}transformDirection(e){for(let t=0,i=this.count;t<i;t++)zt.fromBufferAttribute(this,t),zt.transformDirection(e),this.setXYZ(t,zt.x,zt.y,zt.z);return this}set(e,t=0){return this.array.set(e,t),this}getComponent(e,t){let i=this.array[e*this.itemSize+t];return this.normalized&&(i=Sa(i,this.array)),i}setComponent(e,t,i){return this.normalized&&(i=xn(i,this.array)),this.array[e*this.itemSize+t]=i,this}getX(e){let t=this.array[e*this.itemSize];return this.normalized&&(t=Sa(t,this.array)),t}setX(e,t){return this.normalized&&(t=xn(t,this.array)),this.array[e*this.itemSize]=t,this}getY(e){let t=this.array[e*this.itemSize+1];return this.normalized&&(t=Sa(t,this.array)),t}setY(e,t){return this.normalized&&(t=xn(t,this.array)),this.array[e*this.itemSize+1]=t,this}getZ(e){let t=this.array[e*this.itemSize+2];return this.normalized&&(t=Sa(t,this.array)),t}setZ(e,t){return this.normalized&&(t=xn(t,this.array)),this.array[e*this.itemSize+2]=t,this}getW(e){let t=this.array[e*this.itemSize+3];return this.normalized&&(t=Sa(t,this.array)),t}setW(e,t){return this.normalized&&(t=xn(t,this.array)),this.array[e*this.itemSize+3]=t,this}setXY(e,t,i){return e*=this.itemSize,this.normalized&&(t=xn(t,this.array),i=xn(i,this.array)),this.array[e+0]=t,this.array[e+1]=i,this}setXYZ(e,t,i,r){return e*=this.itemSize,this.normalized&&(t=xn(t,this.array),i=xn(i,this.array),r=xn(r,this.array)),this.array[e+0]=t,this.array[e+1]=i,this.array[e+2]=r,this}setXYZW(e,t,i,r,s){return e*=this.itemSize,this.normalized&&(t=xn(t,this.array),i=xn(i,this.array),r=xn(r,this.array),s=xn(s,this.array)),this.array[e+0]=t,this.array[e+1]=i,this.array[e+2]=r,this.array[e+3]=s,this}onUpload(e){return this.onUploadCallback=e,this}clone(){return new this.constructor(this.array,this.itemSize).copy(this)}toJSON(){let e={itemSize:this.itemSize,type:this.array.constructor.name,array:Array.from(this.array),normalized:this.normalized};return this.name!==""&&(e.name=this.name),this.usage!==km&&(e.usage=this.usage),e}};var Fa=class extends hn{constructor(e,t,i){super(new Uint16Array(e),t,i)}};var ka=class extends hn{constructor(e,t,i){super(new Uint32Array(e),t,i)}};var qn=class extends hn{constructor(e,t,i){super(new Float32Array(e),t,i)}},SC=new Ar,wa=new k,Em=new k,Ss=class{constructor(e=new k,t=-1){this.isSphere=!0,this.center=e,this.radius=t}set(e,t){return this.center.copy(e),this.radius=t,this}setFromPoints(e,t){let i=this.center;t!==void 0?i.copy(t):SC.setFromPoints(e).getCenter(i);let r=0;for(let s=0,o=e.length;s<o;s++)r=Math.max(r,i.distanceToSquared(e[s]));return this.radius=Math.sqrt(r),this}copy(e){return this.center.copy(e.center),this.radius=e.radius,this}isEmpty(){return this.radius<0}makeEmpty(){return this.center.set(0,0,0),this.radius=-1,this}containsPoint(e){return e.distanceToSquared(this.center)<=this.radius*this.radius}distanceToPoint(e){return e.distanceTo(this.center)-this.radius}intersectsSphere(e){let t=this.radius+e.radius;return e.center.distanceToSquared(this.center)<=t*t}intersectsBox(e){return e.intersectsSphere(this)}intersectsPlane(e){return Math.abs(e.distanceToPoint(this.center))<=this.radius}clampPoint(e,t){let i=this.center.distanceToSquared(e);return t.copy(e),i>this.radius*this.radius&&(t.sub(this.center).normalize(),t.multiplyScalar(this.radius).add(this.center)),t}getBoundingBox(e){return this.isEmpty()?(e.makeEmpty(),e):(e.set(this.center,this.center),e.expandByScalar(this.radius),e)}applyMatrix4(e){return this.center.applyMatrix4(e),this.radius=this.radius*e.getMaxScaleOnAxis(),this}translate(e){return this.center.add(e),this}expandByPoint(e){if(this.isEmpty())return this.center.copy(e),this.radius=0,this;wa.subVectors(e,this.center);let t=wa.lengthSq();if(t>this.radius*this.radius){let i=Math.sqrt(t),r=(i-this.radius)*.5;this.center.addScaledVector(wa,r/i),this.radius+=r}return this}union(e){return e.isEmpty()?this:this.isEmpty()?(this.copy(e),this):(this.center.equals(e.center)===!0?this.radius=Math.max(this.radius,e.radius):(Em.subVectors(e.center,this.center).setLength(e.radius),this.expandByPoint(wa.copy(e.center).add(Em)),this.expandByPoint(wa.copy(e.center).sub(Em))),this)}equals(e){return e.center.equals(this.center)&&e.radius===this.radius}clone(){return new this.constructor().copy(this)}toJSON(){return{radius:this.radius,center:this.center.toArray()}}fromJSON(e){return this.radius=e.radius,this.center.fromArray(e.center),this}},EC=0,$n=new Ut,Tm=new sr,yo=new k,An=new Ar,Ca=new Ar,Jt=new k,Nn=class n extends nr{constructor(){super(),this.isBufferGeometry=!0,Object.defineProperty(this,"id",{value:EC++}),this.uuid=nc(),this.name="",this.type="BufferGeometry",this.index=null,this.indirect=null,this.indirectOffset=0,this.attributes={},this.morphAttributes={},this.morphTargetsRelative=!1,this.groups=[],this.boundingBox=null,this.boundingSphere=null,this.drawRange={start:0,count:1/0},this.userData={}}getIndex(){return this.index}setIndex(e){return Array.isArray(e)?this.index=new(lC(e)?ka:Fa)(e,1):this.index=e,this}setIndirect(e,t=0){return this.indirect=e,this.indirectOffset=t,this}getIndirect(){return this.indirect}getAttribute(e){return this.attributes[e]}setAttribute(e,t){return this.attributes[e]=t,this}deleteAttribute(e){return delete this.attributes[e],this}hasAttribute(e){return this.attributes[e]!==void 0}addGroup(e,t,i=0){this.groups.push({start:e,count:t,materialIndex:i})}clearGroups(){this.groups=[]}setDrawRange(e,t){this.drawRange.start=e,this.drawRange.count=t}applyMatrix4(e){let t=this.attributes.position;t!==void 0&&(t.applyMatrix4(e),t.needsUpdate=!0);let i=this.attributes.normal;if(i!==void 0){let s=new Be().getNormalMatrix(e);i.applyNormalMatrix(s),i.needsUpdate=!0}let r=this.attributes.tangent;return r!==void 0&&(r.transformDirection(e),r.needsUpdate=!0),this.boundingBox!==null&&this.computeBoundingBox(),this.boundingSphere!==null&&this.computeBoundingSphere(),this}applyQuaternion(e){return $n.makeRotationFromQuaternion(e),this.applyMatrix4($n),this}rotateX(e){return $n.makeRotationX(e),this.applyMatrix4($n),this}rotateY(e){return $n.makeRotationY(e),this.applyMatrix4($n),this}rotateZ(e){return $n.makeRotationZ(e),this.applyMatrix4($n),this}translate(e,t,i){return $n.makeTranslation(e,t,i),this.applyMatrix4($n),this}scale(e,t,i){return $n.makeScale(e,t,i),this.applyMatrix4($n),this}lookAt(e){return Tm.lookAt(e),Tm.updateMatrix(),this.applyMatrix4(Tm.matrix),this}center(){return this.computeBoundingBox(),this.boundingBox.getCenter(yo).negate(),this.translate(yo.x,yo.y,yo.z),this}setFromPoints(e){let t=this.getAttribute("position");if(t===void 0){let i=[];for(let r=0,s=e.length;r<s;r++){let o=e[r];i.push(o.x,o.y,o.z||0)}this.setAttribute("position",new qn(i,3))}else{let i=Math.min(e.length,t.count);for(let r=0;r<i;r++){let s=e[r];t.setXYZ(r,s.x,s.y,s.z||0)}e.length>t.count&&Ne("BufferGeometry: Buffer size too small for points data. Use .dispose() and create a new geometry."),t.needsUpdate=!0}return this}computeBoundingBox(){this.boundingBox===null&&(this.boundingBox=new Ar);let e=this.attributes.position,t=this.morphAttributes.position;if(e&&e.isGLBufferAttribute){Re("BufferGeometry.computeBoundingBox(): GLBufferAttribute requires a manual bounding box.",this),this.boundingBox.set(new k(-1/0,-1/0,-1/0),new k(1/0,1/0,1/0));return}if(e!==void 0){if(this.boundingBox.setFromBufferAttribute(e),t)for(let i=0,r=t.length;i<r;i++){let s=t[i];An.setFromBufferAttribute(s),this.morphTargetsRelative?(Jt.addVectors(this.boundingBox.min,An.min),this.boundingBox.expandByPoint(Jt),Jt.addVectors(this.boundingBox.max,An.max),this.boundingBox.expandByPoint(Jt)):(this.boundingBox.expandByPoint(An.min),this.boundingBox.expandByPoint(An.max))}}else this.boundingBox.makeEmpty();(isNaN(this.boundingBox.min.x)||isNaN(this.boundingBox.min.y)||isNaN(this.boundingBox.min.z))&&Re('BufferGeometry.computeBoundingBox(): Computed min/max have NaN values. The "position" attribute is likely to have NaN values.',this)}computeBoundingSphere(){this.boundingSphere===null&&(this.boundingSphere=new Ss);let e=this.attributes.position,t=this.morphAttributes.position;if(e&&e.isGLBufferAttribute){Re("BufferGeometry.computeBoundingSphere(): GLBufferAttribute requires a manual bounding sphere.",this),this.boundingSphere.set(new k,1/0);return}if(e){let i=this.boundingSphere.center;if(An.setFromBufferAttribute(e),t)for(let s=0,o=t.length;s<o;s++){let a=t[s];Ca.setFromBufferAttribute(a),this.morphTargetsRelative?(Jt.addVectors(An.min,Ca.min),An.expandByPoint(Jt),Jt.addVectors(An.max,Ca.max),An.expandByPoint(Jt)):(An.expandByPoint(Ca.min),An.expandByPoint(Ca.max))}An.getCenter(i);let r=0;for(let s=0,o=e.count;s<o;s++)Jt.fromBufferAttribute(e,s),r=Math.max(r,i.distanceToSquared(Jt));if(t)for(let s=0,o=t.length;s<o;s++){let a=t[s],c=this.morphTargetsRelative;for(let l=0,u=a.count;l<u;l++)Jt.fromBufferAttribute(a,l),c&&(yo.fromBufferAttribute(e,l),Jt.add(yo)),r=Math.max(r,i.distanceToSquared(Jt))}this.boundingSphere.radius=Math.sqrt(r),isNaN(this.boundingSphere.radius)&&Re('BufferGeometry.computeBoundingSphere(): Computed radius is NaN. The "position" attribute is likely to have NaN values.',this)}}computeTangents(){let e=this.index,t=this.attributes;if(e===null||t.position===void 0||t.normal===void 0||t.uv===void 0){Re("BufferGeometry: .computeTangents() failed. Missing required attributes (index, position, normal or uv)");return}let i=t.position,r=t.normal,s=t.uv;this.hasAttribute("tangent")===!1&&this.setAttribute("tangent",new hn(new Float32Array(4*i.count),4));let o=this.getAttribute("tangent"),a=[],c=[];for(let y=0;y<i.count;y++)a[y]=new k,c[y]=new k;let l=new k,u=new k,f=new k,d=new ht,h=new ht,g=new ht,x=new k,m=new k;function p(y,S,q){l.fromBufferAttribute(i,y),u.fromBufferAttribute(i,S),f.fromBufferAttribute(i,q),d.fromBufferAttribute(s,y),h.fromBufferAttribute(s,S),g.fromBufferAttribute(s,q),u.sub(l),f.sub(l),h.sub(d),g.sub(d);let w=1/(h.x*g.y-g.x*h.y);isFinite(w)&&(x.copy(u).multiplyScalar(g.y).addScaledVector(f,-h.y).multiplyScalar(w),m.copy(f).multiplyScalar(h.x).addScaledVector(u,-g.x).multiplyScalar(w),a[y].add(x),a[S].add(x),a[q].add(x),c[y].add(m),c[S].add(m),c[q].add(m))}let M=this.groups;M.length===0&&(M=[{start:0,count:e.count}]);for(let y=0,S=M.length;y<S;++y){let q=M[y],w=q.start,U=q.count;for(let V=w,W=w+U;V<W;V+=3)p(e.getX(V+0),e.getX(V+1),e.getX(V+2))}let T=new k,E=new k,A=new k,I=new k;function D(y){A.fromBufferAttribute(r,y),I.copy(A);let S=a[y];T.copy(S),T.sub(A.multiplyScalar(A.dot(S))).normalize(),E.crossVectors(I,S);let w=E.dot(c[y])<0?-1:1;o.setXYZW(y,T.x,T.y,T.z,w)}for(let y=0,S=M.length;y<S;++y){let q=M[y],w=q.start,U=q.count;for(let V=w,W=w+U;V<W;V+=3)D(e.getX(V+0)),D(e.getX(V+1)),D(e.getX(V+2))}}computeVertexNormals(){let e=this.index,t=this.getAttribute("position");if(t!==void 0){let i=this.getAttribute("normal");if(i===void 0)i=new hn(new Float32Array(t.count*3),3),this.setAttribute("normal",i);else for(let d=0,h=i.count;d<h;d++)i.setXYZ(d,0,0,0);let r=new k,s=new k,o=new k,a=new k,c=new k,l=new k,u=new k,f=new k;if(e)for(let d=0,h=e.count;d<h;d+=3){let g=e.getX(d+0),x=e.getX(d+1),m=e.getX(d+2);r.fromBufferAttribute(t,g),s.fromBufferAttribute(t,x),o.fromBufferAttribute(t,m),u.subVectors(o,s),f.subVectors(r,s),u.cross(f),a.fromBufferAttribute(i,g),c.fromBufferAttribute(i,x),l.fromBufferAttribute(i,m),a.add(u),c.add(u),l.add(u),i.setXYZ(g,a.x,a.y,a.z),i.setXYZ(x,c.x,c.y,c.z),i.setXYZ(m,l.x,l.y,l.z)}else for(let d=0,h=t.count;d<h;d+=3)r.fromBufferAttribute(t,d+0),s.fromBufferAttribute(t,d+1),o.fromBufferAttribute(t,d+2),u.subVectors(o,s),f.subVectors(r,s),u.cross(f),i.setXYZ(d+0,u.x,u.y,u.z),i.setXYZ(d+1,u.x,u.y,u.z),i.setXYZ(d+2,u.x,u.y,u.z);this.normalizeNormals(),i.needsUpdate=!0}}normalizeNormals(){let e=this.attributes.normal;for(let t=0,i=e.count;t<i;t++)Jt.fromBufferAttribute(e,t),Jt.normalize(),e.setXYZ(t,Jt.x,Jt.y,Jt.z)}toNonIndexed(){function e(a,c){let l=a.array,u=a.itemSize,f=a.normalized,d=new l.constructor(c.length*u),h=0,g=0;for(let x=0,m=c.length;x<m;x++){a.isInterleavedBufferAttribute?h=c[x]*a.data.stride+a.offset:h=c[x]*u;for(let p=0;p<u;p++)d[g++]=l[h++]}return new hn(d,u,f)}if(this.index===null)return Ne("BufferGeometry.toNonIndexed(): BufferGeometry is already non-indexed."),this;let t=new n,i=this.index.array,r=this.attributes;for(let a in r){let c=r[a],l=e(c,i);t.setAttribute(a,l)}let s=this.morphAttributes;for(let a in s){let c=[],l=s[a];for(let u=0,f=l.length;u<f;u++){let d=l[u],h=e(d,i);c.push(h)}t.morphAttributes[a]=c}t.morphTargetsRelative=this.morphTargetsRelative;let o=this.groups;for(let a=0,c=o.length;a<c;a++){let l=o[a];t.addGroup(l.start,l.count,l.materialIndex)}return t}toJSON(){let e={metadata:{version:4.7,type:"BufferGeometry",generator:"BufferGeometry.toJSON"}};if(e.uuid=this.uuid,e.type=this.type,this.name!==""&&(e.name=this.name),Object.keys(this.userData).length>0&&(e.userData=this.userData),this.parameters!==void 0){let c=this.parameters;for(let l in c)c[l]!==void 0&&(e[l]=c[l]);return e}e.data={attributes:{}};let t=this.index;t!==null&&(e.data.index={type:t.array.constructor.name,array:Array.prototype.slice.call(t.array)});let i=this.attributes;for(let c in i){let l=i[c];e.data.attributes[c]=l.toJSON(e.data)}let r={},s=!1;for(let c in this.morphAttributes){let l=this.morphAttributes[c],u=[];for(let f=0,d=l.length;f<d;f++){let h=l[f];u.push(h.toJSON(e.data))}u.length>0&&(r[c]=u,s=!0)}s&&(e.data.morphAttributes=r,e.data.morphTargetsRelative=this.morphTargetsRelative);let o=this.groups;o.length>0&&(e.data.groups=JSON.parse(JSON.stringify(o)));let a=this.boundingSphere;return a!==null&&(e.data.boundingSphere=a.toJSON()),e}clone(){return new this.constructor().copy(this)}copy(e){this.index=null,this.attributes={},this.morphAttributes={},this.groups=[],this.boundingBox=null,this.boundingSphere=null;let t={};this.name=e.name;let i=e.index;i!==null&&this.setIndex(i.clone());let r=e.attributes;for(let l in r){let u=r[l];this.setAttribute(l,u.clone(t))}let s=e.morphAttributes;for(let l in s){let u=[],f=s[l];for(let d=0,h=f.length;d<h;d++)u.push(f[d].clone(t));this.morphAttributes[l]=u}this.morphTargetsRelative=e.morphTargetsRelative;let o=e.groups;for(let l=0,u=o.length;l<u;l++){let f=o[l];this.addGroup(f.start,f.count,f.materialIndex)}let a=e.boundingBox;a!==null&&(this.boundingBox=a.clone());let c=e.boundingSphere;return c!==null&&(this.boundingSphere=c.clone()),this.drawRange.start=e.drawRange.start,this.drawRange.count=e.drawRange.count,this.userData=e.userData,this}dispose(){this.dispatchEvent({type:"dispose"})}};var TC=0,Ci=class extends nr{constructor(){super(),this.isMaterial=!0,Object.defineProperty(this,"id",{value:TC++}),this.uuid=nc(),this.name="",this.type="Material",this.blending=tr,this.side=er,this.vertexColors=!1,this.opacity=1,this.transparent=!1,this.alphaHash=!1,this.blendSrc=pu,this.blendDst=mu,this.blendEquation=Ir,this.blendSrcAlpha=null,this.blendDstAlpha=null,this.blendEquationAlpha=null,this.blendColor=new Xe(0,0,0),this.blendAlpha=0,this.depthFunc=xs,this.depthTest=!0,this.depthWrite=!0,this.stencilWriteMask=255,this.stencilFunc=Fm,this.stencilRef=0,this.stencilFuncMask=255,this.stencilFail=ys,this.stencilZFail=ys,this.stencilZPass=ys,this.stencilWrite=!1,this.clippingPlanes=null,this.clipIntersection=!1,this.clipShadows=!1,this.shadowSide=null,this.colorWrite=!0,this.precision=null,this.polygonOffset=!1,this.polygonOffsetFactor=0,this.polygonOffsetUnits=0,this.dithering=!1,this.alphaToCoverage=!1,this.premultipliedAlpha=!1,this.forceSinglePass=!1,this.allowOverride=!0,this.visible=!0,this.toneMapped=!0,this.userData={},this.version=0,this._alphaTest=0}get alphaTest(){return this._alphaTest}set alphaTest(e){this._alphaTest>0!=e>0&&this.version++,this._alphaTest=e}onBeforeRender(){}onBeforeCompile(){}customProgramCacheKey(){return this.onBeforeCompile.toString()}setValues(e){if(e!==void 0)for(let t in e){let i=e[t];if(i===void 0){Ne(`Material: parameter '${t}' has value of undefined.`);continue}let r=this[t];if(r===void 0){Ne(`Material: '${t}' is not a property of THREE.${this.type}.`);continue}r&&r.isColor?r.set(i):r&&r.isVector3&&i&&i.isVector3?r.copy(i):this[t]=i}}toJSON(e){let t=e===void 0||typeof e=="string";t&&(e={textures:{},images:{}});let i={metadata:{version:4.7,type:"Material",generator:"Material.toJSON"}};i.uuid=this.uuid,i.type=this.type,this.name!==""&&(i.name=this.name),this.color&&this.color.isColor&&(i.color=this.color.getHex()),this.roughness!==void 0&&(i.roughness=this.roughness),this.metalness!==void 0&&(i.metalness=this.metalness),this.sheen!==void 0&&(i.sheen=this.sheen),this.sheenColor&&this.sheenColor.isColor&&(i.sheenColor=this.sheenColor.getHex()),this.sheenRoughness!==void 0&&(i.sheenRoughness=this.sheenRoughness),this.emissive&&this.emissive.isColor&&(i.emissive=this.emissive.getHex()),this.emissiveIntensity!==void 0&&this.emissiveIntensity!==1&&(i.emissiveIntensity=this.emissiveIntensity),this.specular&&this.specular.isColor&&(i.specular=this.specular.getHex()),this.specularIntensity!==void 0&&(i.specularIntensity=this.specularIntensity),this.specularColor&&this.specularColor.isColor&&(i.specularColor=this.specularColor.getHex()),this.shininess!==void 0&&(i.shininess=this.shininess),this.clearcoat!==void 0&&(i.clearcoat=this.clearcoat),this.clearcoatRoughness!==void 0&&(i.clearcoatRoughness=this.clearcoatRoughness),this.clearcoatMap&&this.clearcoatMap.isTexture&&(i.clearcoatMap=this.clearcoatMap.toJSON(e).uuid),this.clearcoatRoughnessMap&&this.clearcoatRoughnessMap.isTexture&&(i.clearcoatRoughnessMap=this.clearcoatRoughnessMap.toJSON(e).uuid),this.clearcoatNormalMap&&this.clearcoatNormalMap.isTexture&&(i.clearcoatNormalMap=this.clearcoatNormalMap.toJSON(e).uuid,i.clearcoatNormalScale=this.clearcoatNormalScale.toArray()),this.sheenColorMap&&this.sheenColorMap.isTexture&&(i.sheenColorMap=this.sheenColorMap.toJSON(e).uuid),this.sheenRoughnessMap&&this.sheenRoughnessMap.isTexture&&(i.sheenRoughnessMap=this.sheenRoughnessMap.toJSON(e).uuid),this.dispersion!==void 0&&(i.dispersion=this.dispersion),this.iridescence!==void 0&&(i.iridescence=this.iridescence),this.iridescenceIOR!==void 0&&(i.iridescenceIOR=this.iridescenceIOR),this.iridescenceThicknessRange!==void 0&&(i.iridescenceThicknessRange=this.iridescenceThicknessRange),this.iridescenceMap&&this.iridescenceMap.isTexture&&(i.iridescenceMap=this.iridescenceMap.toJSON(e).uuid),this.iridescenceThicknessMap&&this.iridescenceThicknessMap.isTexture&&(i.iridescenceThicknessMap=this.iridescenceThicknessMap.toJSON(e).uuid),this.anisotropy!==void 0&&(i.anisotropy=this.anisotropy),this.anisotropyRotation!==void 0&&(i.anisotropyRotation=this.anisotropyRotation),this.anisotropyMap&&this.anisotropyMap.isTexture&&(i.anisotropyMap=this.anisotropyMap.toJSON(e).uuid),this.map&&this.map.isTexture&&(i.map=this.map.toJSON(e).uuid),this.matcap&&this.matcap.isTexture&&(i.matcap=this.matcap.toJSON(e).uuid),this.alphaMap&&this.alphaMap.isTexture&&(i.alphaMap=this.alphaMap.toJSON(e).uuid),this.lightMap&&this.lightMap.isTexture&&(i.lightMap=this.lightMap.toJSON(e).uuid,i.lightMapIntensity=this.lightMapIntensity),this.aoMap&&this.aoMap.isTexture&&(i.aoMap=this.aoMap.toJSON(e).uuid,i.aoMapIntensity=this.aoMapIntensity),this.bumpMap&&this.bumpMap.isTexture&&(i.bumpMap=this.bumpMap.toJSON(e).uuid,i.bumpScale=this.bumpScale),this.normalMap&&this.normalMap.isTexture&&(i.normalMap=this.normalMap.toJSON(e).uuid,i.normalMapType=this.normalMapType,i.normalScale=this.normalScale.toArray()),this.displacementMap&&this.displacementMap.isTexture&&(i.displacementMap=this.displacementMap.toJSON(e).uuid,i.displacementScale=this.displacementScale,i.displacementBias=this.displacementBias),this.roughnessMap&&this.roughnessMap.isTexture&&(i.roughnessMap=this.roughnessMap.toJSON(e).uuid),this.metalnessMap&&this.metalnessMap.isTexture&&(i.metalnessMap=this.metalnessMap.toJSON(e).uuid),this.emissiveMap&&this.emissiveMap.isTexture&&(i.emissiveMap=this.emissiveMap.toJSON(e).uuid),this.specularMap&&this.specularMap.isTexture&&(i.specularMap=this.specularMap.toJSON(e).uuid),this.specularIntensityMap&&this.specularIntensityMap.isTexture&&(i.specularIntensityMap=this.specularIntensityMap.toJSON(e).uuid),this.specularColorMap&&this.specularColorMap.isTexture&&(i.specularColorMap=this.specularColorMap.toJSON(e).uuid),this.envMap&&this.envMap.isTexture&&(i.envMap=this.envMap.toJSON(e).uuid,this.combine!==void 0&&(i.combine=this.combine)),this.envMapRotation!==void 0&&(i.envMapRotation=this.envMapRotation.toArray()),this.envMapIntensity!==void 0&&(i.envMapIntensity=this.envMapIntensity),this.reflectivity!==void 0&&(i.reflectivity=this.reflectivity),this.refractionRatio!==void 0&&(i.refractionRatio=this.refractionRatio),this.gradientMap&&this.gradientMap.isTexture&&(i.gradientMap=this.gradientMap.toJSON(e).uuid),this.transmission!==void 0&&(i.transmission=this.transmission),this.transmissionMap&&this.transmissionMap.isTexture&&(i.transmissionMap=this.transmissionMap.toJSON(e).uuid),this.thickness!==void 0&&(i.thickness=this.thickness),this.thicknessMap&&this.thicknessMap.isTexture&&(i.thicknessMap=this.thicknessMap.toJSON(e).uuid),this.attenuationDistance!==void 0&&this.attenuationDistance!==1/0&&(i.attenuationDistance=this.attenuationDistance),this.attenuationColor!==void 0&&(i.attenuationColor=this.attenuationColor.getHex()),this.size!==void 0&&(i.size=this.size),this.shadowSide!==null&&(i.shadowSide=this.shadowSide),this.sizeAttenuation!==void 0&&(i.sizeAttenuation=this.sizeAttenuation),this.blending!==tr&&(i.blending=this.blending),this.side!==er&&(i.side=this.side),this.vertexColors===!0&&(i.vertexColors=!0),this.opacity<1&&(i.opacity=this.opacity),this.transparent===!0&&(i.transparent=!0),this.blendSrc!==pu&&(i.blendSrc=this.blendSrc),this.blendDst!==mu&&(i.blendDst=this.blendDst),this.blendEquation!==Ir&&(i.blendEquation=this.blendEquation),this.blendSrcAlpha!==null&&(i.blendSrcAlpha=this.blendSrcAlpha),this.blendDstAlpha!==null&&(i.blendDstAlpha=this.blendDstAlpha),this.blendEquationAlpha!==null&&(i.blendEquationAlpha=this.blendEquationAlpha),this.blendColor&&this.blendColor.isColor&&(i.blendColor=this.blendColor.getHex()),this.blendAlpha!==0&&(i.blendAlpha=this.blendAlpha),this.depthFunc!==xs&&(i.depthFunc=this.depthFunc),this.depthTest===!1&&(i.depthTest=this.depthTest),this.depthWrite===!1&&(i.depthWrite=this.depthWrite),this.colorWrite===!1&&(i.colorWrite=this.colorWrite),this.stencilWriteMask!==255&&(i.stencilWriteMask=this.stencilWriteMask),this.stencilFunc!==Fm&&(i.stencilFunc=this.stencilFunc),this.stencilRef!==0&&(i.stencilRef=this.stencilRef),this.stencilFuncMask!==255&&(i.stencilFuncMask=this.stencilFuncMask),this.stencilFail!==ys&&(i.stencilFail=this.stencilFail),this.stencilZFail!==ys&&(i.stencilZFail=this.stencilZFail),this.stencilZPass!==ys&&(i.stencilZPass=this.stencilZPass),this.stencilWrite===!0&&(i.stencilWrite=this.stencilWrite),this.rotation!==void 0&&this.rotation!==0&&(i.rotation=this.rotation),this.polygonOffset===!0&&(i.polygonOffset=!0),this.polygonOffsetFactor!==0&&(i.polygonOffsetFactor=this.polygonOffsetFactor),this.polygonOffsetUnits!==0&&(i.polygonOffsetUnits=this.polygonOffsetUnits),this.linewidth!==void 0&&this.linewidth!==1&&(i.linewidth=this.linewidth),this.dashSize!==void 0&&(i.dashSize=this.dashSize),this.gapSize!==void 0&&(i.gapSize=this.gapSize),this.scale!==void 0&&(i.scale=this.scale),this.dithering===!0&&(i.dithering=!0),this.alphaTest>0&&(i.alphaTest=this.alphaTest),this.alphaHash===!0&&(i.alphaHash=!0),this.alphaToCoverage===!0&&(i.alphaToCoverage=!0),this.premultipliedAlpha===!0&&(i.premultipliedAlpha=!0),this.forceSinglePass===!0&&(i.forceSinglePass=!0),this.allowOverride===!1&&(i.allowOverride=!1),this.wireframe===!0&&(i.wireframe=!0),this.wireframeLinewidth>1&&(i.wireframeLinewidth=this.wireframeLinewidth),this.wireframeLinecap!=="round"&&(i.wireframeLinecap=this.wireframeLinecap),this.wireframeLinejoin!=="round"&&(i.wireframeLinejoin=this.wireframeLinejoin),this.flatShading===!0&&(i.flatShading=!0),this.visible===!1&&(i.visible=!1),this.toneMapped===!1&&(i.toneMapped=!1),this.fog===!1&&(i.fog=!1),Object.keys(this.userData).length>0&&(i.userData=this.userData);function r(s){let o=[];for(let a in s){let c=s[a];delete c.metadata,o.push(c)}return o}if(t){let s=r(e.textures),o=r(e.images);s.length>0&&(i.textures=s),o.length>0&&(i.images=o)}return i}clone(){return new this.constructor().copy(this)}copy(e){this.name=e.name,this.blending=e.blending,this.side=e.side,this.vertexColors=e.vertexColors,this.opacity=e.opacity,this.transparent=e.transparent,this.blendSrc=e.blendSrc,this.blendDst=e.blendDst,this.blendEquation=e.blendEquation,this.blendSrcAlpha=e.blendSrcAlpha,this.blendDstAlpha=e.blendDstAlpha,this.blendEquationAlpha=e.blendEquationAlpha,this.blendColor.copy(e.blendColor),this.blendAlpha=e.blendAlpha,this.depthFunc=e.depthFunc,this.depthTest=e.depthTest,this.depthWrite=e.depthWrite,this.stencilWriteMask=e.stencilWriteMask,this.stencilFunc=e.stencilFunc,this.stencilRef=e.stencilRef,this.stencilFuncMask=e.stencilFuncMask,this.stencilFail=e.stencilFail,this.stencilZFail=e.stencilZFail,this.stencilZPass=e.stencilZPass,this.stencilWrite=e.stencilWrite;let t=e.clippingPlanes,i=null;if(t!==null){let r=t.length;i=new Array(r);for(let s=0;s!==r;++s)i[s]=t[s].clone()}return this.clippingPlanes=i,this.clipIntersection=e.clipIntersection,this.clipShadows=e.clipShadows,this.shadowSide=e.shadowSide,this.colorWrite=e.colorWrite,this.precision=e.precision,this.polygonOffset=e.polygonOffset,this.polygonOffsetFactor=e.polygonOffsetFactor,this.polygonOffsetUnits=e.polygonOffsetUnits,this.dithering=e.dithering,this.alphaTest=e.alphaTest,this.alphaHash=e.alphaHash,this.alphaToCoverage=e.alphaToCoverage,this.premultipliedAlpha=e.premultipliedAlpha,this.forceSinglePass=e.forceSinglePass,this.allowOverride=e.allowOverride,this.visible=e.visible,this.toneMapped=e.toneMapped,this.userData=JSON.parse(JSON.stringify(e.userData)),this}dispose(){this.dispatchEvent({type:"dispose"})}set needsUpdate(e){e===!0&&this.version++}};var Ki=new k,wm=new k,Kl=new k,Tr=new k,Cm=new k,Ql=new k,Im=new k,Ua=class{constructor(e=new k,t=new k(0,0,-1)){this.origin=e,this.direction=t}set(e,t){return this.origin.copy(e),this.direction.copy(t),this}copy(e){return this.origin.copy(e.origin),this.direction.copy(e.direction),this}at(e,t){return t.copy(this.origin).addScaledVector(this.direction,e)}lookAt(e){return this.direction.copy(e).sub(this.origin).normalize(),this}recast(e){return this.origin.copy(this.at(e,Ki)),this}closestPointToPoint(e,t){t.subVectors(e,this.origin);let i=t.dot(this.direction);return i<0?t.copy(this.origin):t.copy(this.origin).addScaledVector(this.direction,i)}distanceToPoint(e){return Math.sqrt(this.distanceSqToPoint(e))}distanceSqToPoint(e){let t=Ki.subVectors(e,this.origin).dot(this.direction);return t<0?this.origin.distanceToSquared(e):(Ki.copy(this.origin).addScaledVector(this.direction,t),Ki.distanceToSquared(e))}distanceSqToSegment(e,t,i,r){wm.copy(e).add(t).multiplyScalar(.5),Kl.copy(t).sub(e).normalize(),Tr.copy(this.origin).sub(wm);let s=e.distanceTo(t)*.5,o=-this.direction.dot(Kl),a=Tr.dot(this.direction),c=-Tr.dot(Kl),l=Tr.lengthSq(),u=Math.abs(1-o*o),f,d,h,g;if(u>0)if(f=o*c-a,d=o*a-c,g=s*u,f>=0)if(d>=-g)if(d<=g){let x=1/u;f*=x,d*=x,h=f*(f+o*d+2*a)+d*(o*f+d+2*c)+l}else d=s,f=Math.max(0,-(o*d+a)),h=-f*f+d*(d+2*c)+l;else d=-s,f=Math.max(0,-(o*d+a)),h=-f*f+d*(d+2*c)+l;else d<=-g?(f=Math.max(0,-(-o*s+a)),d=f>0?-s:Math.min(Math.max(-s,-c),s),h=-f*f+d*(d+2*c)+l):d<=g?(f=0,d=Math.min(Math.max(-s,-c),s),h=d*(d+2*c)+l):(f=Math.max(0,-(o*s+a)),d=f>0?s:Math.min(Math.max(-s,-c),s),h=-f*f+d*(d+2*c)+l);else d=o>0?-s:s,f=Math.max(0,-(o*d+a)),h=-f*f+d*(d+2*c)+l;return i&&i.copy(this.origin).addScaledVector(this.direction,f),r&&r.copy(wm).addScaledVector(Kl,d),h}intersectSphere(e,t){Ki.subVectors(e.center,this.origin);let i=Ki.dot(this.direction),r=Ki.dot(Ki)-i*i,s=e.radius*e.radius;if(r>s)return null;let o=Math.sqrt(s-r),a=i-o,c=i+o;return c<0?null:a<0?this.at(c,t):this.at(a,t)}intersectsSphere(e){return e.radius<0?!1:this.distanceSqToPoint(e.center)<=e.radius*e.radius}distanceToPlane(e){let t=e.normal.dot(this.direction);if(t===0)return e.distanceToPoint(this.origin)===0?0:null;let i=-(this.origin.dot(e.normal)+e.constant)/t;return i>=0?i:null}intersectPlane(e,t){let i=this.distanceToPlane(e);return i===null?null:this.at(i,t)}intersectsPlane(e){let t=e.distanceToPoint(this.origin);return t===0||e.normal.dot(this.direction)*t<0}intersectBox(e,t){let i,r,s,o,a,c,l=1/this.direction.x,u=1/this.direction.y,f=1/this.direction.z,d=this.origin;return l>=0?(i=(e.min.x-d.x)*l,r=(e.max.x-d.x)*l):(i=(e.max.x-d.x)*l,r=(e.min.x-d.x)*l),u>=0?(s=(e.min.y-d.y)*u,o=(e.max.y-d.y)*u):(s=(e.max.y-d.y)*u,o=(e.min.y-d.y)*u),i>o||s>r||((s>i||isNaN(i))&&(i=s),(o<r||isNaN(r))&&(r=o),f>=0?(a=(e.min.z-d.z)*f,c=(e.max.z-d.z)*f):(a=(e.max.z-d.z)*f,c=(e.min.z-d.z)*f),i>c||a>r)||((a>i||i!==i)&&(i=a),(c<r||r!==r)&&(r=c),r<0)?null:this.at(i>=0?i:r,t)}intersectsBox(e){return this.intersectBox(e,Ki)!==null}intersectTriangle(e,t,i,r,s){Cm.subVectors(t,e),Ql.subVectors(i,e),Im.crossVectors(Cm,Ql);let o=this.direction.dot(Im),a;if(o>0){if(r)return null;a=1}else if(o<0)a=-1,o=-o;else return null;Tr.subVectors(this.origin,e);let c=a*this.direction.dot(Ql.crossVectors(Tr,Ql));if(c<0)return null;let l=a*this.direction.dot(Cm.cross(Tr));if(l<0||c+l>o)return null;let u=-a*Tr.dot(Im);return u<0?null:this.at(u/o,s)}applyMatrix4(e){return this.origin.applyMatrix4(e),this.direction.transformDirection(e),this}equals(e){return e.origin.equals(this.origin)&&e.direction.equals(this.direction)}clone(){return new this.constructor().copy(this)}},Ba=class extends Ci{constructor(e){super(),this.isMeshBasicMaterial=!0,this.type="MeshBasicMaterial",this.color=new Xe(16777215),this.map=null,this.lightMap=null,this.lightMapIntensity=1,this.aoMap=null,this.aoMapIntensity=1,this.specularMap=null,this.alphaMap=null,this.envMap=null,this.envMapRotation=new bs,this.combine=Wm,this.reflectivity=1,this.refractionRatio=.98,this.wireframe=!1,this.wireframeLinewidth=1,this.wireframeLinecap="round",this.wireframeLinejoin="round",this.fog=!0,this.setValues(e)}copy(e){return super.copy(e),this.color.copy(e.color),this.map=e.map,this.lightMap=e.lightMap,this.lightMapIntensity=e.lightMapIntensity,this.aoMap=e.aoMap,this.aoMapIntensity=e.aoMapIntensity,this.specularMap=e.specularMap,this.alphaMap=e.alphaMap,this.envMap=e.envMap,this.envMapRotation.copy(e.envMapRotation),this.combine=e.combine,this.reflectivity=e.reflectivity,this.refractionRatio=e.refractionRatio,this.wireframe=e.wireframe,this.wireframeLinewidth=e.wireframeLinewidth,this.wireframeLinecap=e.wireframeLinecap,this.wireframeLinejoin=e.wireframeLinejoin,this.fog=e.fog,this}},Qx=new Ut,gs=new Ua,eu=new Ss,e0=new k,tu=new k,nu=new k,iu=new k,Am=new k,ru=new k,t0=new k,su=new k,Xn=class extends sr{constructor(e=new Nn,t=new Ba){super(),this.isMesh=!0,this.type="Mesh",this.geometry=e,this.material=t,this.morphTargetDictionary=void 0,this.morphTargetInfluences=void 0,this.count=1,this.updateMorphTargets()}copy(e,t){return super.copy(e,t),e.morphTargetInfluences!==void 0&&(this.morphTargetInfluences=e.morphTargetInfluences.slice()),e.morphTargetDictionary!==void 0&&(this.morphTargetDictionary=Object.assign({},e.morphTargetDictionary)),this.material=Array.isArray(e.material)?e.material.slice():e.material,this.geometry=e.geometry,this}updateMorphTargets(){let t=this.geometry.morphAttributes,i=Object.keys(t);if(i.length>0){let r=t[i[0]];if(r!==void 0){this.morphTargetInfluences=[],this.morphTargetDictionary={};for(let s=0,o=r.length;s<o;s++){let a=r[s].name||String(s);this.morphTargetInfluences.push(0),this.morphTargetDictionary[a]=s}}}}getVertexPosition(e,t){let i=this.geometry,r=i.attributes.position,s=i.morphAttributes.position,o=i.morphTargetsRelative;t.fromBufferAttribute(r,e);let a=this.morphTargetInfluences;if(s&&a){ru.set(0,0,0);for(let c=0,l=s.length;c<l;c++){let u=a[c],f=s[c];u!==0&&(Am.fromBufferAttribute(f,e),o?ru.addScaledVector(Am,u):ru.addScaledVector(Am.sub(t),u))}t.add(ru)}return t}raycast(e,t){let i=this.geometry,r=this.material,s=this.matrixWorld;r!==void 0&&(i.boundingSphere===null&&i.computeBoundingSphere(),eu.copy(i.boundingSphere),eu.applyMatrix4(s),gs.copy(e.ray).recast(e.near),!(eu.containsPoint(gs.origin)===!1&&(gs.intersectSphere(eu,e0)===null||gs.origin.distanceToSquared(e0)>(e.far-e.near)**2))&&(Qx.copy(s).invert(),gs.copy(e.ray).applyMatrix4(Qx),!(i.boundingBox!==null&&gs.intersectsBox(i.boundingBox)===!1)&&this._computeIntersections(e,t,gs)))}_computeIntersections(e,t,i){let r,s=this.geometry,o=this.material,a=s.index,c=s.attributes.position,l=s.attributes.uv,u=s.attributes.uv1,f=s.attributes.normal,d=s.groups,h=s.drawRange;if(a!==null)if(Array.isArray(o))for(let g=0,x=d.length;g<x;g++){let m=d[g],p=o[m.materialIndex],M=Math.max(m.start,h.start),T=Math.min(a.count,Math.min(m.start+m.count,h.start+h.count));for(let E=M,A=T;E<A;E+=3){let I=a.getX(E),D=a.getX(E+1),y=a.getX(E+2);r=ou(this,p,e,i,l,u,f,I,D,y),r&&(r.faceIndex=Math.floor(E/3),r.face.materialIndex=m.materialIndex,t.push(r))}}else{let g=Math.max(0,h.start),x=Math.min(a.count,h.start+h.count);for(let m=g,p=x;m<p;m+=3){let M=a.getX(m),T=a.getX(m+1),E=a.getX(m+2);r=ou(this,o,e,i,l,u,f,M,T,E),r&&(r.faceIndex=Math.floor(m/3),t.push(r))}}else if(c!==void 0)if(Array.isArray(o))for(let g=0,x=d.length;g<x;g++){let m=d[g],p=o[m.materialIndex],M=Math.max(m.start,h.start),T=Math.min(c.count,Math.min(m.start+m.count,h.start+h.count));for(let E=M,A=T;E<A;E+=3){let I=E,D=E+1,y=E+2;r=ou(this,p,e,i,l,u,f,I,D,y),r&&(r.faceIndex=Math.floor(E/3),r.face.materialIndex=m.materialIndex,t.push(r))}}else{let g=Math.max(0,h.start),x=Math.min(c.count,h.start+h.count);for(let m=g,p=x;m<p;m+=3){let M=m,T=m+1,E=m+2;r=ou(this,o,e,i,l,u,f,M,T,E),r&&(r.faceIndex=Math.floor(m/3),t.push(r))}}}};function wC(n,e,t,i,r,s,o,a){let c;if(e.side===vn?c=i.intersectTriangle(o,s,r,!0,a):c=i.intersectTriangle(r,s,o,e.side===er,a),c===null)return null;su.copy(a),su.applyMatrix4(n.matrixWorld);let l=t.ray.origin.distanceTo(su);return l<t.near||l>t.far?null:{distance:l,point:su.clone(),object:n}}function ou(n,e,t,i,r,s,o,a,c,l){n.getVertexPosition(a,tu),n.getVertexPosition(c,nu),n.getVertexPosition(l,iu);let u=wC(n,e,t,i,tu,nu,iu,t0);if(u){let f=new k;Cr.getBarycoord(t0,tu,nu,iu,f),r&&(u.uv=Cr.getInterpolatedAttribute(r,a,c,l,f,new ht)),s&&(u.uv1=Cr.getInterpolatedAttribute(s,a,c,l,f,new ht)),o&&(u.normal=Cr.getInterpolatedAttribute(o,a,c,l,f,new k),u.normal.dot(i.direction)>0&&u.normal.multiplyScalar(-1));let d={a,b:c,c:l,normal:new k,materialIndex:0};Cr.getNormal(tu,nu,iu,d.normal),u.face=d,u.barycoord=f}return u}var Du=class extends rr{constructor(e=null,t=1,i=1,r,s,o,a,c,l=Kt,u=Kt,f,d){super(null,o,a,c,l,u,r,s,f,d),this.isDataTexture=!0,this.image={data:e,width:t,height:i},this.generateMipmaps=!1,this.flipY=!1,this.unpackAlignment=1}};var Dm=new k,CC=new k,IC=new Be,Si=class{constructor(e=new k(1,0,0),t=0){this.isPlane=!0,this.normal=e,this.constant=t}set(e,t){return this.normal.copy(e),this.constant=t,this}setComponents(e,t,i,r){return this.normal.set(e,t,i),this.constant=r,this}setFromNormalAndCoplanarPoint(e,t){return this.normal.copy(e),this.constant=-t.dot(this.normal),this}setFromCoplanarPoints(e,t,i){let r=Dm.subVectors(i,t).cross(CC.subVectors(e,t)).normalize();return this.setFromNormalAndCoplanarPoint(r,e),this}copy(e){return this.normal.copy(e.normal),this.constant=e.constant,this}normalize(){let e=1/this.normal.length();return this.normal.multiplyScalar(e),this.constant*=e,this}negate(){return this.constant*=-1,this.normal.negate(),this}distanceToPoint(e){return this.normal.dot(e)+this.constant}distanceToSphere(e){return this.distanceToPoint(e.center)-e.radius}projectPoint(e,t){return t.copy(e).addScaledVector(this.normal,-this.distanceToPoint(e))}intersectLine(e,t){let i=e.delta(Dm),r=this.normal.dot(i);if(r===0)return this.distanceToPoint(e.start)===0?t.copy(e.start):null;let s=-(e.start.dot(this.normal)+this.constant)/r;return s<0||s>1?null:t.copy(e.start).addScaledVector(i,s)}intersectsLine(e){let t=this.distanceToPoint(e.start),i=this.distanceToPoint(e.end);return t<0&&i>0||i<0&&t>0}intersectsBox(e){return e.intersectsPlane(this)}intersectsSphere(e){return e.intersectsPlane(this)}coplanarPoint(e){return e.copy(this.normal).multiplyScalar(-this.constant)}applyMatrix4(e,t){let i=t||IC.getNormalMatrix(e),r=this.coplanarPoint(Dm).applyMatrix4(e),s=this.normal.applyMatrix3(i).normalize();return this.constant=-r.dot(s),this}translate(e){return this.constant-=e.dot(this.normal),this}equals(e){return e.normal.equals(this.normal)&&e.constant===this.constant}clone(){return new this.constructor().copy(this)}},vs=new Ss,AC=new ht(.5,.5),au=new k,Va=class{constructor(e=new Si,t=new Si,i=new Si,r=new Si,s=new Si,o=new Si){this.planes=[e,t,i,r,s,o]}set(e,t,i,r,s,o){let a=this.planes;return a[0].copy(e),a[1].copy(t),a[2].copy(i),a[3].copy(r),a[4].copy(s),a[5].copy(o),this}copy(e){let t=this.planes;for(let i=0;i<6;i++)t[i].copy(e.planes[i]);return this}setFromProjectionMatrix(e,t=ui,i=!1){let r=this.planes,s=e.elements,o=s[0],a=s[1],c=s[2],l=s[3],u=s[4],f=s[5],d=s[6],h=s[7],g=s[8],x=s[9],m=s[10],p=s[11],M=s[12],T=s[13],E=s[14],A=s[15];if(r[0].setComponents(l-o,h-u,p-g,A-M).normalize(),r[1].setComponents(l+o,h+u,p+g,A+M).normalize(),r[2].setComponents(l+a,h+f,p+x,A+T).normalize(),r[3].setComponents(l-a,h-f,p-x,A-T).normalize(),i)r[4].setComponents(c,d,m,E).normalize(),r[5].setComponents(l-c,h-d,p-m,A-E).normalize();else if(r[4].setComponents(l-c,h-d,p-m,A-E).normalize(),t===ui)r[5].setComponents(l+c,h+d,p+m,A+E).normalize();else if(t===Da)r[5].setComponents(c,d,m,E).normalize();else throw new Error("THREE.Frustum.setFromProjectionMatrix(): Invalid coordinate system: "+t);return this}intersectsObject(e){if(e.boundingSphere!==void 0)e.boundingSphere===null&&e.computeBoundingSphere(),vs.copy(e.boundingSphere).applyMatrix4(e.matrixWorld);else{let t=e.geometry;t.boundingSphere===null&&t.computeBoundingSphere(),vs.copy(t.boundingSphere).applyMatrix4(e.matrixWorld)}return this.intersectsSphere(vs)}intersectsSprite(e){vs.center.set(0,0,0);let t=AC.distanceTo(e.center);return vs.radius=.7071067811865476+t,vs.applyMatrix4(e.matrixWorld),this.intersectsSphere(vs)}intersectsSphere(e){let t=this.planes,i=e.center,r=-e.radius;for(let s=0;s<6;s++)if(t[s].distanceToPoint(i)<r)return!1;return!0}intersectsBox(e){let t=this.planes;for(let i=0;i<6;i++){let r=t[i];if(au.x=r.normal.x>0?e.max.x:e.min.x,au.y=r.normal.y>0?e.max.y:e.min.y,au.z=r.normal.z>0?e.max.z:e.min.z,r.distanceToPoint(au)<0)return!1}return!0}containsPoint(e){let t=this.planes;for(let i=0;i<6;i++)if(t[i].distanceToPoint(e)<0)return!1;return!0}clone(){return new this.constructor().copy(this)}};var To=class extends Ci{constructor(e){super(),this.isPointsMaterial=!0,this.type="PointsMaterial",this.color=new Xe(16777215),this.map=null,this.alphaMap=null,this.size=1,this.sizeAttenuation=!0,this.fog=!0,this.setValues(e)}copy(e){return super.copy(e),this.color.copy(e.color),this.map=e.map,this.alphaMap=e.alphaMap,this.size=e.size,this.sizeAttenuation=e.sizeAttenuation,this.fog=e.fog,this}},n0=new Ut,Um=new Ua,cu=new Ss,lu=new k,Ha=class extends sr{constructor(e=new Nn,t=new To){super(),this.isPoints=!0,this.type="Points",this.geometry=e,this.material=t,this.morphTargetDictionary=void 0,this.morphTargetInfluences=void 0,this.updateMorphTargets()}copy(e,t){return super.copy(e,t),this.material=Array.isArray(e.material)?e.material.slice():e.material,this.geometry=e.geometry,this}raycast(e,t){let i=this.geometry,r=this.matrixWorld,s=e.params.Points.threshold,o=i.drawRange;if(i.boundingSphere===null&&i.computeBoundingSphere(),cu.copy(i.boundingSphere),cu.applyMatrix4(r),cu.radius+=s,e.ray.intersectsSphere(cu)===!1)return;n0.copy(r).invert(),Um.copy(e.ray).applyMatrix4(n0);let a=s/((this.scale.x+this.scale.y+this.scale.z)/3),c=a*a,l=i.index,f=i.attributes.position;if(l!==null){let d=Math.max(0,o.start),h=Math.min(l.count,o.start+o.count);for(let g=d,x=h;g<x;g++){let m=l.getX(g);lu.fromBufferAttribute(f,m),i0(lu,m,c,r,e,t,this)}}else{let d=Math.max(0,o.start),h=Math.min(f.count,o.start+o.count);for(let g=d,x=h;g<x;g++)lu.fromBufferAttribute(f,g),i0(lu,g,c,r,e,t,this)}}updateMorphTargets(){let t=this.geometry.morphAttributes,i=Object.keys(t);if(i.length>0){let r=t[i[0]];if(r!==void 0){this.morphTargetInfluences=[],this.morphTargetDictionary={};for(let s=0,o=r.length;s<o;s++){let a=r[s].name||String(s);this.morphTargetInfluences.push(0),this.morphTargetDictionary[a]=s}}}}};function i0(n,e,t,i,r,s,o){let a=Um.distanceSqToPoint(n);if(a<t){let c=new k;Um.closestPointToPoint(n,c),c.applyMatrix4(i);let l=r.ray.origin.distanceTo(c);if(l<r.near||l>r.far)return;s.push({distance:l,distanceToRay:Math.sqrt(a),point:c,index:e,face:null,faceIndex:null,barycoord:null,object:o})}}var za=class extends rr{constructor(e=[],t=Lr,i,r,s,o,a,c,l,u){super(e,t,i,r,s,o,a,c,l,u),this.isCubeTexture=!0,this.flipY=!1}get images(){return this.image}set images(e){this.image=e}};var Dr=class extends rr{constructor(e,t,i=fi,r,s,o,a=Kt,c=Kt,l,u=Ti,f=1){if(u!==Ti&&u!==Fr)throw new Error("DepthTexture format must be either THREE.DepthFormat or THREE.DepthStencilFormat");let d={width:e,height:t,depth:f};super(d,r,s,o,a,c,u,i,l),this.isDepthTexture=!0,this.flipY=!1,this.generateMipmaps=!1,this.compareFunction=null}copy(e){return super.copy(e),this.source=new So(Object.assign({},e.image)),this.compareFunction=e.compareFunction,this}toJSON(e){let t=super.toJSON(e);return this.compareFunction!==null&&(t.compareFunction=this.compareFunction),t}},Ru=class extends Dr{constructor(e,t=fi,i=Lr,r,s,o=Kt,a=Kt,c,l=Ti){let u={width:e,height:e,depth:1},f=[u,u,u,u,u,u];super(e,e,t,i,r,s,o,a,c,l),this.image=f,this.isCubeDepthTexture=!0,this.isCubeTexture=!0}get images(){return this.image}set images(e){this.image=e}},Ga=class extends rr{constructor(e=null){super(),this.sourceTexture=e,this.isExternalTexture=!0}copy(e){return super.copy(e),this.sourceTexture=e.sourceTexture,this}},wo=class n extends Nn{constructor(e=1,t=1,i=1,r=1,s=1,o=1){super(),this.type="BoxGeometry",this.parameters={width:e,height:t,depth:i,widthSegments:r,heightSegments:s,depthSegments:o};let a=this;r=Math.floor(r),s=Math.floor(s),o=Math.floor(o);let c=[],l=[],u=[],f=[],d=0,h=0;g("z","y","x",-1,-1,i,t,e,o,s,0),g("z","y","x",1,-1,i,t,-e,o,s,1),g("x","z","y",1,1,e,i,t,r,o,2),g("x","z","y",1,-1,e,i,-t,r,o,3),g("x","y","z",1,-1,e,t,i,r,s,4),g("x","y","z",-1,-1,e,t,-i,r,s,5),this.setIndex(c),this.setAttribute("position",new qn(l,3)),this.setAttribute("normal",new qn(u,3)),this.setAttribute("uv",new qn(f,2));function g(x,m,p,M,T,E,A,I,D,y,S){let q=E/D,w=A/y,U=E/2,V=A/2,W=I/2,B=D+1,H=y+1,L=0,Q=0,Z=new k;for(let ue=0;ue<H;ue++){let ge=ue*w-V;for(let fe=0;fe<B;fe++){let He=fe*q-U;Z[x]=He*M,Z[m]=ge*T,Z[p]=W,l.push(Z.x,Z.y,Z.z),Z[x]=0,Z[m]=0,Z[p]=I>0?1:-1,u.push(Z.x,Z.y,Z.z),f.push(fe/D),f.push(1-ue/y),L+=1}}for(let ue=0;ue<y;ue++)for(let ge=0;ge<D;ge++){let fe=d+ge+B*ue,He=d+ge+B*(ue+1),Tt=d+(ge+1)+B*(ue+1),St=d+(ge+1)+B*ue;c.push(fe,He,St),c.push(He,Tt,St),Q+=6}a.addGroup(h,Q,S),h+=Q,d+=L}}copy(e){return super.copy(e),this.parameters=Object.assign({},e.parameters),this}static fromJSON(e){return new n(e.width,e.height,e.depth,e.widthSegments,e.heightSegments,e.depthSegments)}};var Wa=class n extends Nn{constructor(e=1,t=1,i=1,r=1){super(),this.type="PlaneGeometry",this.parameters={width:e,height:t,widthSegments:i,heightSegments:r};let s=e/2,o=t/2,a=Math.floor(i),c=Math.floor(r),l=a+1,u=c+1,f=e/a,d=t/c,h=[],g=[],x=[],m=[];for(let p=0;p<u;p++){let M=p*d-o;for(let T=0;T<l;T++){let E=T*f-s;g.push(E,-M,0),x.push(0,0,1),m.push(T/a),m.push(1-p/c)}}for(let p=0;p<c;p++)for(let M=0;M<a;M++){let T=M+l*p,E=M+l*(p+1),A=M+1+l*(p+1),I=M+1+l*p;h.push(T,E,I),h.push(E,A,I)}this.setIndex(h),this.setAttribute("position",new qn(g,3)),this.setAttribute("normal",new qn(x,3)),this.setAttribute("uv",new qn(m,2))}copy(e){return super.copy(e),this.parameters=Object.assign({},e.parameters),this}static fromJSON(e){return new n(e.width,e.height,e.widthSegments,e.heightSegments)}};function ws(n){let e={};for(let t in n){e[t]={};for(let i in n[t]){let r=n[t][i];r&&(r.isColor||r.isMatrix3||r.isMatrix4||r.isVector2||r.isVector3||r.isVector4||r.isTexture||r.isQuaternion)?r.isRenderTargetTexture?(Ne("UniformsUtils: Textures of render targets cannot be cloned via cloneUniforms() or mergeUniforms()."),e[t][i]=null):e[t][i]=r.clone():Array.isArray(r)?e[t][i]=r.slice():e[t][i]=r}}return e}function pn(n){let e={};for(let t=0;t<n.length;t++){let i=ws(n[t]);for(let r in i)e[r]=i[r]}return e}function DC(n){let e=[];for(let t=0;t<n.length;t++)e.push(n[t].clone());return e}function ag(n){let e=n.getRenderTarget();return e===null?n.outputColorSpace:e.isXRRenderTarget===!0?e.texture.colorSpace:tt.workingColorSpace}var W0={clone:ws,merge:pn},RC=`void main() {
	gl_Position = projectionMatrix * modelViewMatrix * vec4( position, 1.0 );
}`,NC=`void main() {
	gl_FragColor = vec4( 1.0, 0.0, 0.0, 1.0 );
}`,Pn=class extends Ci{constructor(e){super(),this.isShaderMaterial=!0,this.type="ShaderMaterial",this.defines={},this.uniforms={},this.uniformsGroups=[],this.vertexShader=RC,this.fragmentShader=NC,this.linewidth=1,this.wireframe=!1,this.wireframeLinewidth=1,this.fog=!1,this.lights=!1,this.clipping=!1,this.forceSinglePass=!0,this.extensions={clipCullDistance:!1,multiDraw:!1},this.defaultAttributeValues={color:[1,1,1],uv:[0,0],uv1:[0,0]},this.index0AttributeName=void 0,this.uniformsNeedUpdate=!1,this.glslVersion=null,e!==void 0&&this.setValues(e)}copy(e){return super.copy(e),this.fragmentShader=e.fragmentShader,this.vertexShader=e.vertexShader,this.uniforms=ws(e.uniforms),this.uniformsGroups=DC(e.uniformsGroups),this.defines=Object.assign({},e.defines),this.wireframe=e.wireframe,this.wireframeLinewidth=e.wireframeLinewidth,this.fog=e.fog,this.lights=e.lights,this.clipping=e.clipping,this.extensions=Object.assign({},e.extensions),this.glslVersion=e.glslVersion,this.defaultAttributeValues=Object.assign({},e.defaultAttributeValues),this.index0AttributeName=e.index0AttributeName,this.uniformsNeedUpdate=e.uniformsNeedUpdate,this}toJSON(e){let t=super.toJSON(e);t.glslVersion=this.glslVersion,t.uniforms={};for(let r in this.uniforms){let o=this.uniforms[r].value;o&&o.isTexture?t.uniforms[r]={type:"t",value:o.toJSON(e).uuid}:o&&o.isColor?t.uniforms[r]={type:"c",value:o.getHex()}:o&&o.isVector2?t.uniforms[r]={type:"v2",value:o.toArray()}:o&&o.isVector3?t.uniforms[r]={type:"v3",value:o.toArray()}:o&&o.isVector4?t.uniforms[r]={type:"v4",value:o.toArray()}:o&&o.isMatrix3?t.uniforms[r]={type:"m3",value:o.toArray()}:o&&o.isMatrix4?t.uniforms[r]={type:"m4",value:o.toArray()}:t.uniforms[r]={value:o}}Object.keys(this.defines).length>0&&(t.defines=this.defines),t.vertexShader=this.vertexShader,t.fragmentShader=this.fragmentShader,t.lights=this.lights,t.clipping=this.clipping;let i={};for(let r in this.extensions)this.extensions[r]===!0&&(i[r]=!0);return Object.keys(i).length>0&&(t.extensions=i),t}},Nu=class extends Pn{constructor(e){super(e),this.isRawShaderMaterial=!0,this.type="RawShaderMaterial"}};var Pu=class extends Ci{constructor(e){super(),this.isMeshDepthMaterial=!0,this.type="MeshDepthMaterial",this.depthPacking=D0,this.map=null,this.alphaMap=null,this.displacementMap=null,this.displacementScale=1,this.displacementBias=0,this.wireframe=!1,this.wireframeLinewidth=1,this.setValues(e)}copy(e){return super.copy(e),this.depthPacking=e.depthPacking,this.map=e.map,this.alphaMap=e.alphaMap,this.displacementMap=e.displacementMap,this.displacementScale=e.displacementScale,this.displacementBias=e.displacementBias,this.wireframe=e.wireframe,this.wireframeLinewidth=e.wireframeLinewidth,this}},Lu=class extends Ci{constructor(e){super(),this.isMeshDistanceMaterial=!0,this.type="MeshDistanceMaterial",this.map=null,this.alphaMap=null,this.displacementMap=null,this.displacementScale=1,this.displacementBias=0,this.setValues(e)}copy(e){return super.copy(e),this.map=e.map,this.alphaMap=e.alphaMap,this.displacementMap=e.displacementMap,this.displacementScale=e.displacementScale,this.displacementBias=e.displacementBias,this}};function uu(n,e){return!n||n.constructor===e?n:typeof e.BYTES_PER_ELEMENT=="number"?new e(n):Array.prototype.slice.call(n)}var Rr=class{constructor(e,t,i,r){this.parameterPositions=e,this._cachedIndex=0,this.resultBuffer=r!==void 0?r:new t.constructor(i),this.sampleValues=t,this.valueSize=i,this.settings=null,this.DefaultSettings_={}}evaluate(e){let t=this.parameterPositions,i=this._cachedIndex,r=t[i],s=t[i-1];n:{e:{let o;t:{i:if(!(e<r)){for(let a=i+2;;){if(r===void 0){if(e<s)break i;return i=t.length,this._cachedIndex=i,this.copySampleValue_(i-1)}if(i===a)break;if(s=r,r=t[++i],e<r)break e}o=t.length;break t}if(!(e>=s)){let a=t[1];e<a&&(i=2,s=a);for(let c=i-2;;){if(s===void 0)return this._cachedIndex=0,this.copySampleValue_(0);if(i===c)break;if(r=s,s=t[--i-1],e>=s)break e}o=i,i=0;break t}break n}for(;i<o;){let a=i+o>>>1;e<t[a]?o=a:i=a+1}if(r=t[i],s=t[i-1],s===void 0)return this._cachedIndex=0,this.copySampleValue_(0);if(r===void 0)return i=t.length,this._cachedIndex=i,this.copySampleValue_(i-1)}this._cachedIndex=i,this.intervalChanged_(i,s,r)}return this.interpolate_(i,s,e,r)}getSettings_(){return this.settings||this.DefaultSettings_}copySampleValue_(e){let t=this.resultBuffer,i=this.sampleValues,r=this.valueSize,s=e*r;for(let o=0;o!==r;++o)t[o]=i[s+o];return t}interpolate_(){throw new Error("call to abstract method")}intervalChanged_(){}},Ou=class extends Rr{constructor(e,t,i,r){super(e,t,i,r),this._weightPrev=-0,this._offsetPrev=-0,this._weightNext=-0,this._offsetNext=-0,this.DefaultSettings_={endingStart:Pm,endingEnd:Pm}}intervalChanged_(e,t,i){let r=this.parameterPositions,s=e-2,o=e+1,a=r[s],c=r[o];if(a===void 0)switch(this.getSettings_().endingStart){case Lm:s=e,a=2*t-i;break;case Om:s=r.length-2,a=t+r[s]-r[s+1];break;default:s=e,a=i}if(c===void 0)switch(this.getSettings_().endingEnd){case Lm:o=e,c=2*i-t;break;case Om:o=1,c=i+r[1]-r[0];break;default:o=e-1,c=t}let l=(i-t)*.5,u=this.valueSize;this._weightPrev=l/(t-a),this._weightNext=l/(c-i),this._offsetPrev=s*u,this._offsetNext=o*u}interpolate_(e,t,i,r){let s=this.resultBuffer,o=this.sampleValues,a=this.valueSize,c=e*a,l=c-a,u=this._offsetPrev,f=this._offsetNext,d=this._weightPrev,h=this._weightNext,g=(i-t)/(r-t),x=g*g,m=x*g,p=-d*m+2*d*x-d*g,M=(1+d)*m+(-1.5-2*d)*x+(-.5+d)*g+1,T=(-1-h)*m+(1.5+h)*x+.5*g,E=h*m-h*x;for(let A=0;A!==a;++A)s[A]=p*o[u+A]+M*o[l+A]+T*o[c+A]+E*o[f+A];return s}},Fu=class extends Rr{constructor(e,t,i,r){super(e,t,i,r)}interpolate_(e,t,i,r){let s=this.resultBuffer,o=this.sampleValues,a=this.valueSize,c=e*a,l=c-a,u=(i-t)/(r-t),f=1-u;for(let d=0;d!==a;++d)s[d]=o[l+d]*f+o[c+d]*u;return s}},ku=class extends Rr{constructor(e,t,i,r){super(e,t,i,r)}interpolate_(e){return this.copySampleValue_(e-1)}},Uu=class extends Rr{interpolate_(e,t,i,r){let s=this.resultBuffer,o=this.sampleValues,a=this.valueSize,c=e*a,l=c-a,u=this.settings||this.DefaultSettings_,f=u.inTangents,d=u.outTangents;if(!f||!d){let x=(i-t)/(r-t),m=1-x;for(let p=0;p!==a;++p)s[p]=o[l+p]*m+o[c+p]*x;return s}let h=a*2,g=e-1;for(let x=0;x!==a;++x){let m=o[l+x],p=o[c+x],M=g*h+x*2,T=d[M],E=d[M+1],A=e*h+x*2,I=f[A],D=f[A+1],y=(i-t)/(r-t),S,q,w,U,V;for(let W=0;W<8;W++){S=y*y,q=S*y,w=1-y,U=w*w,V=U*w;let H=V*t+3*U*y*T+3*w*S*I+q*r-i;if(Math.abs(H)<1e-10)break;let L=3*U*(T-t)+6*w*y*(I-T)+3*S*(r-I);if(Math.abs(L)<1e-10)break;y=y-H/L,y=Math.max(0,Math.min(1,y))}s[x]=V*m+3*U*y*E+3*w*S*D+q*p}return s}},Ln=class{constructor(e,t,i,r){if(e===void 0)throw new Error("THREE.KeyframeTrack: track name is undefined");if(t===void 0||t.length===0)throw new Error("THREE.KeyframeTrack: no keyframes in track named "+e);this.name=e,this.times=uu(t,this.TimeBufferType),this.values=uu(i,this.ValueBufferType),this.setInterpolation(r||this.DefaultInterpolation)}static toJSON(e){let t=e.constructor,i;if(t.toJSON!==this.toJSON)i=t.toJSON(e);else{i={name:e.name,times:uu(e.times,Array),values:uu(e.values,Array)};let r=e.getInterpolation();r!==e.DefaultInterpolation&&(i.interpolation=r)}return i.type=e.ValueTypeName,i}InterpolantFactoryMethodDiscrete(e){return new ku(this.times,this.values,this.getValueSize(),e)}InterpolantFactoryMethodLinear(e){return new Fu(this.times,this.values,this.getValueSize(),e)}InterpolantFactoryMethodSmooth(e){return new Ou(this.times,this.values,this.getValueSize(),e)}InterpolantFactoryMethodBezier(e){let t=new Uu(this.times,this.values,this.getValueSize(),e);return this.settings&&(t.settings=this.settings),t}setInterpolation(e){let t;switch(e){case Ia:t=this.InterpolantFactoryMethodDiscrete;break;case Tu:t=this.InterpolantFactoryMethodLinear;break;case hu:t=this.InterpolantFactoryMethodSmooth;break;case Nm:t=this.InterpolantFactoryMethodBezier;break}if(t===void 0){let i="unsupported interpolation for "+this.ValueTypeName+" keyframe track named "+this.name;if(this.createInterpolant===void 0)if(e!==this.DefaultInterpolation)this.setInterpolation(this.DefaultInterpolation);else throw new Error(i);return Ne("KeyframeTrack:",i),this}return this.createInterpolant=t,this}getInterpolation(){switch(this.createInterpolant){case this.InterpolantFactoryMethodDiscrete:return Ia;case this.InterpolantFactoryMethodLinear:return Tu;case this.InterpolantFactoryMethodSmooth:return hu;case this.InterpolantFactoryMethodBezier:return Nm}}getValueSize(){return this.values.length/this.times.length}shift(e){if(e!==0){let t=this.times;for(let i=0,r=t.length;i!==r;++i)t[i]+=e}return this}scale(e){if(e!==1){let t=this.times;for(let i=0,r=t.length;i!==r;++i)t[i]*=e}return this}trim(e,t){let i=this.times,r=i.length,s=0,o=r-1;for(;s!==r&&i[s]<e;)++s;for(;o!==-1&&i[o]>t;)--o;if(++o,s!==0||o!==r){s>=o&&(o=Math.max(o,1),s=o-1);let a=this.getValueSize();this.times=i.slice(s,o),this.values=this.values.slice(s*a,o*a)}return this}validate(){let e=!0,t=this.getValueSize();t-Math.floor(t)!==0&&(Re("KeyframeTrack: Invalid value size in track.",this),e=!1);let i=this.times,r=this.values,s=i.length;s===0&&(Re("KeyframeTrack: Track is empty.",this),e=!1);let o=null;for(let a=0;a!==s;a++){let c=i[a];if(typeof c=="number"&&isNaN(c)){Re("KeyframeTrack: Time is not a valid number.",this,a,c),e=!1;break}if(o!==null&&o>c){Re("KeyframeTrack: Out of order keys.",this,a,c,o),e=!1;break}o=c}if(r!==void 0&&uC(r))for(let a=0,c=r.length;a!==c;++a){let l=r[a];if(isNaN(l)){Re("KeyframeTrack: Value is not a valid number.",this,a,l),e=!1;break}}return e}optimize(){let e=this.times.slice(),t=this.values.slice(),i=this.getValueSize(),r=this.getInterpolation()===hu,s=e.length-1,o=1;for(let a=1;a<s;++a){let c=!1,l=e[a],u=e[a+1];if(l!==u&&(a!==1||l!==e[0]))if(r)c=!0;else{let f=a*i,d=f-i,h=f+i;for(let g=0;g!==i;++g){let x=t[f+g];if(x!==t[d+g]||x!==t[h+g]){c=!0;break}}}if(c){if(a!==o){e[o]=e[a];let f=a*i,d=o*i;for(let h=0;h!==i;++h)t[d+h]=t[f+h]}++o}}if(s>0){e[o]=e[s];for(let a=s*i,c=o*i,l=0;l!==i;++l)t[c+l]=t[a+l];++o}return o!==e.length?(this.times=e.slice(0,o),this.values=t.slice(0,o*i)):(this.times=e,this.values=t),this}clone(){let e=this.times.slice(),t=this.values.slice(),i=this.constructor,r=new i(this.name,e,t);return r.createInterpolant=this.createInterpolant,r}};Ln.prototype.ValueTypeName="";Ln.prototype.TimeBufferType=Float32Array;Ln.prototype.ValueBufferType=Float32Array;Ln.prototype.DefaultInterpolation=Tu;var Nr=class extends Ln{constructor(e,t,i){super(e,t,i)}};Nr.prototype.ValueTypeName="bool";Nr.prototype.ValueBufferType=Array;Nr.prototype.DefaultInterpolation=Ia;Nr.prototype.InterpolantFactoryMethodLinear=void 0;Nr.prototype.InterpolantFactoryMethodSmooth=void 0;var Bu=class extends Ln{constructor(e,t,i,r){super(e,t,i,r)}};Bu.prototype.ValueTypeName="color";var Vu=class extends Ln{constructor(e,t,i,r){super(e,t,i,r)}};Vu.prototype.ValueTypeName="number";var Hu=class extends Rr{constructor(e,t,i,r){super(e,t,i,r)}interpolate_(e,t,i,r){let s=this.resultBuffer,o=this.sampleValues,a=this.valueSize,c=(i-t)/(r-t),l=e*a;for(let u=l+a;l!==u;l+=4)wi.slerpFlat(s,0,o,l-a,o,l,c);return s}},ja=class extends Ln{constructor(e,t,i,r){super(e,t,i,r)}InterpolantFactoryMethodLinear(e){return new Hu(this.times,this.values,this.getValueSize(),e)}};ja.prototype.ValueTypeName="quaternion";ja.prototype.InterpolantFactoryMethodSmooth=void 0;var Pr=class extends Ln{constructor(e,t,i){super(e,t,i)}};Pr.prototype.ValueTypeName="string";Pr.prototype.ValueBufferType=Array;Pr.prototype.DefaultInterpolation=Ia;Pr.prototype.InterpolantFactoryMethodLinear=void 0;Pr.prototype.InterpolantFactoryMethodSmooth=void 0;var zu=class extends Ln{constructor(e,t,i,r){super(e,t,i,r)}};zu.prototype.ValueTypeName="vector";var Gu=class extends sr{constructor(e,t=1){super(),this.isLight=!0,this.type="Light",this.color=new Xe(e),this.intensity=t}dispose(){this.dispatchEvent({type:"dispose"})}copy(e,t){return super.copy(e,t),this.color.copy(e.color),this.intensity=e.intensity,this}toJSON(e){let t=super.toJSON(e);return t.object.color=this.color.getHex(),t.object.intensity=this.intensity,t}};var du=new k,fu=new wi,bi=new k,$a=class extends sr{constructor(){super(),this.isCamera=!0,this.type="Camera",this.matrixWorldInverse=new Ut,this.projectionMatrix=new Ut,this.projectionMatrixInverse=new Ut,this.coordinateSystem=ui,this._reversedDepth=!1}get reversedDepth(){return this._reversedDepth}copy(e,t){return super.copy(e,t),this.matrixWorldInverse.copy(e.matrixWorldInverse),this.projectionMatrix.copy(e.projectionMatrix),this.projectionMatrixInverse.copy(e.projectionMatrixInverse),this.coordinateSystem=e.coordinateSystem,this}getWorldDirection(e){return super.getWorldDirection(e).negate()}updateMatrixWorld(e){super.updateMatrixWorld(e),this.matrixWorld.decompose(du,fu,bi),bi.x===1&&bi.y===1&&bi.z===1?this.matrixWorldInverse.copy(this.matrixWorld).invert():this.matrixWorldInverse.compose(du,fu,bi.set(1,1,1)).invert()}updateWorldMatrix(e,t){super.updateWorldMatrix(e,t),this.matrixWorld.decompose(du,fu,bi),bi.x===1&&bi.y===1&&bi.z===1?this.matrixWorldInverse.copy(this.matrixWorld).invert():this.matrixWorldInverse.compose(du,fu,bi.set(1,1,1)).invert()}clone(){return new this.constructor().copy(this)}},wr=new k,r0=new ht,s0=new ht,fn=class extends $a{constructor(e=50,t=1,i=.1,r=2e3){super(),this.isPerspectiveCamera=!0,this.type="PerspectiveCamera",this.fov=e,this.zoom=1,this.near=i,this.far=r,this.focus=10,this.aspect=t,this.view=null,this.filmGauge=35,this.filmOffset=0,this.updateProjectionMatrix()}copy(e,t){return super.copy(e,t),this.fov=e.fov,this.zoom=e.zoom,this.near=e.near,this.far=e.far,this.focus=e.focus,this.aspect=e.aspect,this.view=e.view===null?null:Object.assign({},e.view),this.filmGauge=e.filmGauge,this.filmOffset=e.filmOffset,this}setFocalLength(e){let t=.5*this.getFilmHeight()/e;this.fov=wu*2*Math.atan(t),this.updateProjectionMatrix()}getFocalLength(){let e=Math.tan(cm*.5*this.fov);return .5*this.getFilmHeight()/e}getEffectiveFOV(){return wu*2*Math.atan(Math.tan(cm*.5*this.fov)/this.zoom)}getFilmWidth(){return this.filmGauge*Math.min(this.aspect,1)}getFilmHeight(){return this.filmGauge/Math.max(this.aspect,1)}getViewBounds(e,t,i){wr.set(-1,-1,.5).applyMatrix4(this.projectionMatrixInverse),t.set(wr.x,wr.y).multiplyScalar(-e/wr.z),wr.set(1,1,.5).applyMatrix4(this.projectionMatrixInverse),i.set(wr.x,wr.y).multiplyScalar(-e/wr.z)}getViewSize(e,t){return this.getViewBounds(e,r0,s0),t.subVectors(s0,r0)}setViewOffset(e,t,i,r,s,o){this.aspect=e/t,this.view===null&&(this.view={enabled:!0,fullWidth:1,fullHeight:1,offsetX:0,offsetY:0,width:1,height:1}),this.view.enabled=!0,this.view.fullWidth=e,this.view.fullHeight=t,this.view.offsetX=i,this.view.offsetY=r,this.view.width=s,this.view.height=o,this.updateProjectionMatrix()}clearViewOffset(){this.view!==null&&(this.view.enabled=!1),this.updateProjectionMatrix()}updateProjectionMatrix(){let e=this.near,t=e*Math.tan(cm*.5*this.fov)/this.zoom,i=2*t,r=this.aspect*i,s=-.5*r,o=this.view;if(this.view!==null&&this.view.enabled){let c=o.fullWidth,l=o.fullHeight;s+=o.offsetX*r/c,t-=o.offsetY*i/l,r*=o.width/c,i*=o.height/l}let a=this.filmOffset;a!==0&&(s+=e*a/this.getFilmWidth()),this.projectionMatrix.makePerspective(s,s+r,t,t-i,e,this.far,this.coordinateSystem,this.reversedDepth),this.projectionMatrixInverse.copy(this.projectionMatrix).invert()}toJSON(e){let t=super.toJSON(e);return t.object.fov=this.fov,t.object.zoom=this.zoom,t.object.near=this.near,t.object.far=this.far,t.object.focus=this.focus,t.object.aspect=this.aspect,this.view!==null&&(t.object.view=Object.assign({},this.view)),t.object.filmGauge=this.filmGauge,t.object.filmOffset=this.filmOffset,t}};var qa=class extends $a{constructor(e=-1,t=1,i=1,r=-1,s=.1,o=2e3){super(),this.isOrthographicCamera=!0,this.type="OrthographicCamera",this.zoom=1,this.view=null,this.left=e,this.right=t,this.top=i,this.bottom=r,this.near=s,this.far=o,this.updateProjectionMatrix()}copy(e,t){return super.copy(e,t),this.left=e.left,this.right=e.right,this.top=e.top,this.bottom=e.bottom,this.near=e.near,this.far=e.far,this.zoom=e.zoom,this.view=e.view===null?null:Object.assign({},e.view),this}setViewOffset(e,t,i,r,s,o){this.view===null&&(this.view={enabled:!0,fullWidth:1,fullHeight:1,offsetX:0,offsetY:0,width:1,height:1}),this.view.enabled=!0,this.view.fullWidth=e,this.view.fullHeight=t,this.view.offsetX=i,this.view.offsetY=r,this.view.width=s,this.view.height=o,this.updateProjectionMatrix()}clearViewOffset(){this.view!==null&&(this.view.enabled=!1),this.updateProjectionMatrix()}updateProjectionMatrix(){let e=(this.right-this.left)/(2*this.zoom),t=(this.top-this.bottom)/(2*this.zoom),i=(this.right+this.left)/2,r=(this.top+this.bottom)/2,s=i-e,o=i+e,a=r+t,c=r-t;if(this.view!==null&&this.view.enabled){let l=(this.right-this.left)/this.view.fullWidth/this.zoom,u=(this.top-this.bottom)/this.view.fullHeight/this.zoom;s+=l*this.view.offsetX,o=s+l*this.view.width,a-=u*this.view.offsetY,c=a-u*this.view.height}this.projectionMatrix.makeOrthographic(s,o,a,c,this.near,this.far,this.coordinateSystem,this.reversedDepth),this.projectionMatrixInverse.copy(this.projectionMatrix).invert()}toJSON(e){let t=super.toJSON(e);return t.object.zoom=this.zoom,t.object.left=this.left,t.object.right=this.right,t.object.top=this.top,t.object.bottom=this.bottom,t.object.near=this.near,t.object.far=this.far,this.view!==null&&(t.object.view=Object.assign({},this.view)),t}};var Xa=class extends Gu{constructor(e,t){super(e,t),this.isAmbientLight=!0,this.type="AmbientLight"}};var _o=-90,xo=1,Wu=class extends sr{constructor(e,t,i){super(),this.type="CubeCamera",this.renderTarget=i,this.coordinateSystem=null,this.activeMipmapLevel=0;let r=new fn(_o,xo,e,t);r.layers=this.layers,this.add(r);let s=new fn(_o,xo,e,t);s.layers=this.layers,this.add(s);let o=new fn(_o,xo,e,t);o.layers=this.layers,this.add(o);let a=new fn(_o,xo,e,t);a.layers=this.layers,this.add(a);let c=new fn(_o,xo,e,t);c.layers=this.layers,this.add(c);let l=new fn(_o,xo,e,t);l.layers=this.layers,this.add(l)}updateCoordinateSystem(){let e=this.coordinateSystem,t=this.children.concat(),[i,r,s,o,a,c]=t;for(let l of t)this.remove(l);if(e===ui)i.up.set(0,1,0),i.lookAt(1,0,0),r.up.set(0,1,0),r.lookAt(-1,0,0),s.up.set(0,0,-1),s.lookAt(0,1,0),o.up.set(0,0,1),o.lookAt(0,-1,0),a.up.set(0,1,0),a.lookAt(0,0,1),c.up.set(0,1,0),c.lookAt(0,0,-1);else if(e===Da)i.up.set(0,-1,0),i.lookAt(-1,0,0),r.up.set(0,-1,0),r.lookAt(1,0,0),s.up.set(0,0,1),s.lookAt(0,1,0),o.up.set(0,0,-1),o.lookAt(0,-1,0),a.up.set(0,-1,0),a.lookAt(0,0,1),c.up.set(0,-1,0),c.lookAt(0,0,-1);else throw new Error("THREE.CubeCamera.updateCoordinateSystem(): Invalid coordinate system: "+e);for(let l of t)this.add(l),l.updateMatrixWorld()}update(e,t){this.parent===null&&this.updateMatrixWorld();let{renderTarget:i,activeMipmapLevel:r}=this;this.coordinateSystem!==e.coordinateSystem&&(this.coordinateSystem=e.coordinateSystem,this.updateCoordinateSystem());let[s,o,a,c,l,u]=this.children,f=e.getRenderTarget(),d=e.getActiveCubeFace(),h=e.getActiveMipmapLevel(),g=e.xr.enabled;e.xr.enabled=!1;let x=i.texture.generateMipmaps;i.texture.generateMipmaps=!1;let m=!1;e.isWebGLRenderer===!0?m=e.state.buffers.depth.getReversed():m=e.reversedDepthBuffer,e.setRenderTarget(i,0,r),m&&e.autoClear===!1&&e.clearDepth(),e.render(t,s),e.setRenderTarget(i,1,r),m&&e.autoClear===!1&&e.clearDepth(),e.render(t,o),e.setRenderTarget(i,2,r),m&&e.autoClear===!1&&e.clearDepth(),e.render(t,a),e.setRenderTarget(i,3,r),m&&e.autoClear===!1&&e.clearDepth(),e.render(t,c),e.setRenderTarget(i,4,r),m&&e.autoClear===!1&&e.clearDepth(),e.render(t,l),i.texture.generateMipmaps=x,e.setRenderTarget(i,5,r),m&&e.autoClear===!1&&e.clearDepth(),e.render(t,u),e.setRenderTarget(f,d,h),e.xr.enabled=g,i.texture.needsPMREMUpdate=!0}},ju=class extends fn{constructor(e=[]){super(),this.isArrayCamera=!0,this.isMultiViewCamera=!1,this.cameras=e}};var cg="\\[\\]\\.:\\/",PC=new RegExp("["+cg+"]","g"),lg="[^"+cg+"]",LC="[^"+cg.replace("\\.","")+"]",OC=/((?:WC+[\/:])*)/.source.replace("WC",lg),FC=/(WCOD+)?/.source.replace("WCOD",LC),kC=/(?:\.(WC+)(?:\[(.+)\])?)?/.source.replace("WC",lg),UC=/\.(WC+)(?:\[(.+)\])?/.source.replace("WC",lg),BC=new RegExp("^"+OC+FC+kC+UC+"$"),VC=["material","materials","bones","map"],Bm=class{constructor(e,t,i){let r=i||Nt.parseTrackName(t);this._targetGroup=e,this._bindings=e.subscribe_(t,r)}getValue(e,t){this.bind();let i=this._targetGroup.nCachedObjects_,r=this._bindings[i];r!==void 0&&r.getValue(e,t)}setValue(e,t){let i=this._bindings;for(let r=this._targetGroup.nCachedObjects_,s=i.length;r!==s;++r)i[r].setValue(e,t)}bind(){let e=this._bindings;for(let t=this._targetGroup.nCachedObjects_,i=e.length;t!==i;++t)e[t].bind()}unbind(){let e=this._bindings;for(let t=this._targetGroup.nCachedObjects_,i=e.length;t!==i;++t)e[t].unbind()}},Nt=(()=>{class n{constructor(t,i,r){this.path=i,this.parsedPath=r||n.parseTrackName(i),this.node=n.findNode(t,this.parsedPath.nodeName),this.rootNode=t,this.getValue=this._getValue_unbound,this.setValue=this._setValue_unbound}static create(t,i,r){return t&&t.isAnimationObjectGroup?new n.Composite(t,i,r):new n(t,i,r)}static sanitizeNodeName(t){return t.replace(/\s/g,"_").replace(PC,"")}static parseTrackName(t){let i=BC.exec(t);if(i===null)throw new Error("PropertyBinding: Cannot parse trackName: "+t);let r={nodeName:i[2],objectName:i[3],objectIndex:i[4],propertyName:i[5],propertyIndex:i[6]},s=r.nodeName&&r.nodeName.lastIndexOf(".");if(s!==void 0&&s!==-1){let o=r.nodeName.substring(s+1);VC.indexOf(o)!==-1&&(r.nodeName=r.nodeName.substring(0,s),r.objectName=o)}if(r.propertyName===null||r.propertyName.length===0)throw new Error("PropertyBinding: can not parse propertyName from trackName: "+t);return r}static findNode(t,i){if(i===void 0||i===""||i==="."||i===-1||i===t.name||i===t.uuid)return t;if(t.skeleton){let r=t.skeleton.getBoneByName(i);if(r!==void 0)return r}if(t.children){let r=function(o){for(let a=0;a<o.length;a++){let c=o[a];if(c.name===i||c.uuid===i)return c;let l=r(c.children);if(l)return l}return null},s=r(t.children);if(s)return s}return null}_getValue_unavailable(){}_setValue_unavailable(){}_getValue_direct(t,i){t[i]=this.targetObject[this.propertyName]}_getValue_array(t,i){let r=this.resolvedProperty;for(let s=0,o=r.length;s!==o;++s)t[i++]=r[s]}_getValue_arrayElement(t,i){t[i]=this.resolvedProperty[this.propertyIndex]}_getValue_toArray(t,i){this.resolvedProperty.toArray(t,i)}_setValue_direct(t,i){this.targetObject[this.propertyName]=t[i]}_setValue_direct_setNeedsUpdate(t,i){this.targetObject[this.propertyName]=t[i],this.targetObject.needsUpdate=!0}_setValue_direct_setMatrixWorldNeedsUpdate(t,i){this.targetObject[this.propertyName]=t[i],this.targetObject.matrixWorldNeedsUpdate=!0}_setValue_array(t,i){let r=this.resolvedProperty;for(let s=0,o=r.length;s!==o;++s)r[s]=t[i++]}_setValue_array_setNeedsUpdate(t,i){let r=this.resolvedProperty;for(let s=0,o=r.length;s!==o;++s)r[s]=t[i++];this.targetObject.needsUpdate=!0}_setValue_array_setMatrixWorldNeedsUpdate(t,i){let r=this.resolvedProperty;for(let s=0,o=r.length;s!==o;++s)r[s]=t[i++];this.targetObject.matrixWorldNeedsUpdate=!0}_setValue_arrayElement(t,i){this.resolvedProperty[this.propertyIndex]=t[i]}_setValue_arrayElement_setNeedsUpdate(t,i){this.resolvedProperty[this.propertyIndex]=t[i],this.targetObject.needsUpdate=!0}_setValue_arrayElement_setMatrixWorldNeedsUpdate(t,i){this.resolvedProperty[this.propertyIndex]=t[i],this.targetObject.matrixWorldNeedsUpdate=!0}_setValue_fromArray(t,i){this.resolvedProperty.fromArray(t,i)}_setValue_fromArray_setNeedsUpdate(t,i){this.resolvedProperty.fromArray(t,i),this.targetObject.needsUpdate=!0}_setValue_fromArray_setMatrixWorldNeedsUpdate(t,i){this.resolvedProperty.fromArray(t,i),this.targetObject.matrixWorldNeedsUpdate=!0}_getValue_unbound(t,i){this.bind(),this.getValue(t,i)}_setValue_unbound(t,i){this.bind(),this.setValue(t,i)}bind(){let t=this.node,i=this.parsedPath,r=i.objectName,s=i.propertyName,o=i.propertyIndex;if(t||(t=n.findNode(this.rootNode,i.nodeName),this.node=t),this.getValue=this._getValue_unavailable,this.setValue=this._setValue_unavailable,!t){Ne("PropertyBinding: No target node found for track: "+this.path+".");return}if(r){let u=i.objectIndex;switch(r){case"materials":if(!t.material){Re("PropertyBinding: Can not bind to material as node does not have a material.",this);return}if(!t.material.materials){Re("PropertyBinding: Can not bind to material.materials as node.material does not have a materials array.",this);return}t=t.material.materials;break;case"bones":if(!t.skeleton){Re("PropertyBinding: Can not bind to bones as node does not have a skeleton.",this);return}t=t.skeleton.bones;for(let f=0;f<t.length;f++)if(t[f].name===u){u=f;break}break;case"map":if("map"in t){t=t.map;break}if(!t.material){Re("PropertyBinding: Can not bind to material as node does not have a material.",this);return}if(!t.material.map){Re("PropertyBinding: Can not bind to material.map as node.material does not have a map.",this);return}t=t.material.map;break;default:if(t[r]===void 0){Re("PropertyBinding: Can not bind to objectName of node undefined.",this);return}t=t[r]}if(u!==void 0){if(t[u]===void 0){Re("PropertyBinding: Trying to bind to objectIndex of objectName, but is undefined.",this,t);return}t=t[u]}}let a=t[s];if(a===void 0){let u=i.nodeName;Re("PropertyBinding: Trying to update property for track: "+u+"."+s+" but it wasn't found.",t);return}let c=this.Versioning.None;this.targetObject=t,t.isMaterial===!0?c=this.Versioning.NeedsUpdate:t.isObject3D===!0&&(c=this.Versioning.MatrixWorldNeedsUpdate);let l=this.BindingType.Direct;if(o!==void 0){if(s==="morphTargetInfluences"){if(!t.geometry){Re("PropertyBinding: Can not bind to morphTargetInfluences because node does not have a geometry.",this);return}if(!t.geometry.morphAttributes){Re("PropertyBinding: Can not bind to morphTargetInfluences because node does not have a geometry.morphAttributes.",this);return}t.morphTargetDictionary[o]!==void 0&&(o=t.morphTargetDictionary[o])}l=this.BindingType.ArrayElement,this.resolvedProperty=a,this.propertyIndex=o}else a.fromArray!==void 0&&a.toArray!==void 0?(l=this.BindingType.HasFromToArray,this.resolvedProperty=a):Array.isArray(a)?(l=this.BindingType.EntireArray,this.resolvedProperty=a):this.propertyName=s;this.getValue=this.GetterByBindingType[l],this.setValue=this.SetterByBindingTypeAndVersioning[l][c]}unbind(){this.node=null,this.getValue=this._getValue_unbound,this.setValue=this._setValue_unbound}}return n.Composite=Bm,n})();Nt.prototype.BindingType={Direct:0,EntireArray:1,ArrayElement:2,HasFromToArray:3};Nt.prototype.Versioning={None:0,NeedsUpdate:1,MatrixWorldNeedsUpdate:2};Nt.prototype.GetterByBindingType=[Nt.prototype._getValue_direct,Nt.prototype._getValue_array,Nt.prototype._getValue_arrayElement,Nt.prototype._getValue_toArray];Nt.prototype.SetterByBindingTypeAndVersioning=[[Nt.prototype._setValue_direct,Nt.prototype._setValue_direct_setNeedsUpdate,Nt.prototype._setValue_direct_setMatrixWorldNeedsUpdate],[Nt.prototype._setValue_array,Nt.prototype._setValue_array_setNeedsUpdate,Nt.prototype._setValue_array_setMatrixWorldNeedsUpdate],[Nt.prototype._setValue_arrayElement,Nt.prototype._setValue_arrayElement_setNeedsUpdate,Nt.prototype._setValue_arrayElement_setMatrixWorldNeedsUpdate],[Nt.prototype._setValue_fromArray,Nt.prototype._setValue_fromArray_setNeedsUpdate,Nt.prototype._setValue_fromArray_setMatrixWorldNeedsUpdate]];var tH=new Float32Array(1);function ug(n,e,t,i){let r=HC(i);switch(t){case ng:return n*e;case rg:return n*e/r.components*r.byteLength;case Ku:return n*e/r.components*r.byteLength;case Ts:return n*e*2/r.components*r.byteLength;case Qu:return n*e*2/r.components*r.byteLength;case ig:return n*e*3/r.components*r.byteLength;case Yn:return n*e*4/r.components*r.byteLength;case ed:return n*e*4/r.components*r.byteLength;case Ka:case Qa:return Math.floor((n+3)/4)*Math.floor((e+3)/4)*8;case ec:case tc:return Math.floor((n+3)/4)*Math.floor((e+3)/4)*16;case nd:case rd:return Math.max(n,16)*Math.max(e,8)/4;case td:case id:return Math.max(n,8)*Math.max(e,8)/2;case sd:case od:case cd:case ld:return Math.floor((n+3)/4)*Math.floor((e+3)/4)*8;case ad:case ud:case dd:return Math.floor((n+3)/4)*Math.floor((e+3)/4)*16;case fd:return Math.floor((n+3)/4)*Math.floor((e+3)/4)*16;case hd:return Math.floor((n+4)/5)*Math.floor((e+3)/4)*16;case pd:return Math.floor((n+4)/5)*Math.floor((e+4)/5)*16;case md:return Math.floor((n+5)/6)*Math.floor((e+4)/5)*16;case gd:return Math.floor((n+5)/6)*Math.floor((e+5)/6)*16;case vd:return Math.floor((n+7)/8)*Math.floor((e+4)/5)*16;case yd:return Math.floor((n+7)/8)*Math.floor((e+5)/6)*16;case _d:return Math.floor((n+7)/8)*Math.floor((e+7)/8)*16;case xd:return Math.floor((n+9)/10)*Math.floor((e+4)/5)*16;case Md:return Math.floor((n+9)/10)*Math.floor((e+5)/6)*16;case bd:return Math.floor((n+9)/10)*Math.floor((e+7)/8)*16;case Sd:return Math.floor((n+9)/10)*Math.floor((e+9)/10)*16;case Ed:return Math.floor((n+11)/12)*Math.floor((e+9)/10)*16;case Td:return Math.floor((n+11)/12)*Math.floor((e+11)/12)*16;case wd:case Cd:case Id:return Math.ceil(n/4)*Math.ceil(e/4)*16;case Ad:case Dd:return Math.ceil(n/4)*Math.ceil(e/4)*8;case Rd:case Nd:return Math.ceil(n/4)*Math.ceil(e/4)*16}throw new Error(`Unable to determine texture byte length for ${t} format.`)}function HC(n){switch(n){case On:case Km:return{byteLength:1,components:1};case Io:case Qm:case Di:return{byteLength:2,components:1};case Zu:case Ju:return{byteLength:2,components:4};case fi:case Yu:case hi:return{byteLength:4,components:1};case eg:case tg:return{byteLength:4,components:3}}throw new Error(`Unknown texture type ${n}.`)}typeof __THREE_DEVTOOLS__<"u"&&__THREE_DEVTOOLS__.dispatchEvent(new CustomEvent("register",{detail:{revision:"183"}}));typeof window<"u"&&(window.__THREE__?Ne("WARNING: Multiple instances of Three.js being imported."):window.__THREE__="183");function hM(){let n=null,e=!1,t=null,i=null;function r(s,o){t(s,o),i=n.requestAnimationFrame(r)}return{start:function(){e!==!0&&t!==null&&(i=n.requestAnimationFrame(r),e=!0)},stop:function(){n.cancelAnimationFrame(i),e=!1},setAnimationLoop:function(s){t=s},setContext:function(s){n=s}}}function GC(n){let e=new WeakMap;function t(a,c){let l=a.array,u=a.usage,f=l.byteLength,d=n.createBuffer();n.bindBuffer(c,d),n.bufferData(c,l,u),a.onUploadCallback();let h;if(l instanceof Float32Array)h=n.FLOAT;else if(typeof Float16Array<"u"&&l instanceof Float16Array)h=n.HALF_FLOAT;else if(l instanceof Uint16Array)a.isFloat16BufferAttribute?h=n.HALF_FLOAT:h=n.UNSIGNED_SHORT;else if(l instanceof Int16Array)h=n.SHORT;else if(l instanceof Uint32Array)h=n.UNSIGNED_INT;else if(l instanceof Int32Array)h=n.INT;else if(l instanceof Int8Array)h=n.BYTE;else if(l instanceof Uint8Array)h=n.UNSIGNED_BYTE;else if(l instanceof Uint8ClampedArray)h=n.UNSIGNED_BYTE;else throw new Error("THREE.WebGLAttributes: Unsupported buffer data format: "+l);return{buffer:d,type:h,bytesPerElement:l.BYTES_PER_ELEMENT,version:a.version,size:f}}function i(a,c,l){let u=c.array,f=c.updateRanges;if(n.bindBuffer(l,a),f.length===0)n.bufferSubData(l,0,u);else{f.sort((h,g)=>h.start-g.start);let d=0;for(let h=1;h<f.length;h++){let g=f[d],x=f[h];x.start<=g.start+g.count+1?g.count=Math.max(g.count,x.start+x.count-g.start):(++d,f[d]=x)}f.length=d+1;for(let h=0,g=f.length;h<g;h++){let x=f[h];n.bufferSubData(l,x.start*u.BYTES_PER_ELEMENT,u,x.start,x.count)}c.clearUpdateRanges()}c.onUploadCallback()}function r(a){return a.isInterleavedBufferAttribute&&(a=a.data),e.get(a)}function s(a){a.isInterleavedBufferAttribute&&(a=a.data);let c=e.get(a);c&&(n.deleteBuffer(c.buffer),e.delete(a))}function o(a,c){if(a.isInterleavedBufferAttribute&&(a=a.data),a.isGLBufferAttribute){let u=e.get(a);(!u||u.version<a.version)&&e.set(a,{buffer:a.buffer,type:a.type,bytesPerElement:a.elementSize,version:a.version});return}let l=e.get(a);if(l===void 0)e.set(a,t(a,c));else if(l.version<a.version){if(l.size!==a.array.byteLength)throw new Error("THREE.WebGLAttributes: The size of the buffer attribute's array buffer does not match the original size. Resizing buffer attributes is not supported.");i(l.buffer,a,c),l.version=a.version}}return{get:r,remove:s,update:o}}var WC=`#ifdef USE_ALPHAHASH
	if ( diffuseColor.a < getAlphaHashThreshold( vPosition ) ) discard;
#endif`,jC=`#ifdef USE_ALPHAHASH
	const float ALPHA_HASH_SCALE = 0.05;
	float hash2D( vec2 value ) {
		return fract( 1.0e4 * sin( 17.0 * value.x + 0.1 * value.y ) * ( 0.1 + abs( sin( 13.0 * value.y + value.x ) ) ) );
	}
	float hash3D( vec3 value ) {
		return hash2D( vec2( hash2D( value.xy ), value.z ) );
	}
	float getAlphaHashThreshold( vec3 position ) {
		float maxDeriv = max(
			length( dFdx( position.xyz ) ),
			length( dFdy( position.xyz ) )
		);
		float pixScale = 1.0 / ( ALPHA_HASH_SCALE * maxDeriv );
		vec2 pixScales = vec2(
			exp2( floor( log2( pixScale ) ) ),
			exp2( ceil( log2( pixScale ) ) )
		);
		vec2 alpha = vec2(
			hash3D( floor( pixScales.x * position.xyz ) ),
			hash3D( floor( pixScales.y * position.xyz ) )
		);
		float lerpFactor = fract( log2( pixScale ) );
		float x = ( 1.0 - lerpFactor ) * alpha.x + lerpFactor * alpha.y;
		float a = min( lerpFactor, 1.0 - lerpFactor );
		vec3 cases = vec3(
			x * x / ( 2.0 * a * ( 1.0 - a ) ),
			( x - 0.5 * a ) / ( 1.0 - a ),
			1.0 - ( ( 1.0 - x ) * ( 1.0 - x ) / ( 2.0 * a * ( 1.0 - a ) ) )
		);
		float threshold = ( x < ( 1.0 - a ) )
			? ( ( x < a ) ? cases.x : cases.y )
			: cases.z;
		return clamp( threshold , 1.0e-6, 1.0 );
	}
#endif`,$C=`#ifdef USE_ALPHAMAP
	diffuseColor.a *= texture2D( alphaMap, vAlphaMapUv ).g;
#endif`,qC=`#ifdef USE_ALPHAMAP
	uniform sampler2D alphaMap;
#endif`,XC=`#ifdef USE_ALPHATEST
	#ifdef ALPHA_TO_COVERAGE
	diffuseColor.a = smoothstep( alphaTest, alphaTest + fwidth( diffuseColor.a ), diffuseColor.a );
	if ( diffuseColor.a == 0.0 ) discard;
	#else
	if ( diffuseColor.a < alphaTest ) discard;
	#endif
#endif`,YC=`#ifdef USE_ALPHATEST
	uniform float alphaTest;
#endif`,ZC=`#ifdef USE_AOMAP
	float ambientOcclusion = ( texture2D( aoMap, vAoMapUv ).r - 1.0 ) * aoMapIntensity + 1.0;
	reflectedLight.indirectDiffuse *= ambientOcclusion;
	#if defined( USE_CLEARCOAT ) 
		clearcoatSpecularIndirect *= ambientOcclusion;
	#endif
	#if defined( USE_SHEEN ) 
		sheenSpecularIndirect *= ambientOcclusion;
	#endif
	#if defined( USE_ENVMAP ) && defined( STANDARD )
		float dotNV = saturate( dot( geometryNormal, geometryViewDir ) );
		reflectedLight.indirectSpecular *= computeSpecularOcclusion( dotNV, ambientOcclusion, material.roughness );
	#endif
#endif`,JC=`#ifdef USE_AOMAP
	uniform sampler2D aoMap;
	uniform float aoMapIntensity;
#endif`,KC=`#ifdef USE_BATCHING
	#if ! defined( GL_ANGLE_multi_draw )
	#define gl_DrawID _gl_DrawID
	uniform int _gl_DrawID;
	#endif
	uniform highp sampler2D batchingTexture;
	uniform highp usampler2D batchingIdTexture;
	mat4 getBatchingMatrix( const in float i ) {
		int size = textureSize( batchingTexture, 0 ).x;
		int j = int( i ) * 4;
		int x = j % size;
		int y = j / size;
		vec4 v1 = texelFetch( batchingTexture, ivec2( x, y ), 0 );
		vec4 v2 = texelFetch( batchingTexture, ivec2( x + 1, y ), 0 );
		vec4 v3 = texelFetch( batchingTexture, ivec2( x + 2, y ), 0 );
		vec4 v4 = texelFetch( batchingTexture, ivec2( x + 3, y ), 0 );
		return mat4( v1, v2, v3, v4 );
	}
	float getIndirectIndex( const in int i ) {
		int size = textureSize( batchingIdTexture, 0 ).x;
		int x = i % size;
		int y = i / size;
		return float( texelFetch( batchingIdTexture, ivec2( x, y ), 0 ).r );
	}
#endif
#ifdef USE_BATCHING_COLOR
	uniform sampler2D batchingColorTexture;
	vec4 getBatchingColor( const in float i ) {
		int size = textureSize( batchingColorTexture, 0 ).x;
		int j = int( i );
		int x = j % size;
		int y = j / size;
		return texelFetch( batchingColorTexture, ivec2( x, y ), 0 );
	}
#endif`,QC=`#ifdef USE_BATCHING
	mat4 batchingMatrix = getBatchingMatrix( getIndirectIndex( gl_DrawID ) );
#endif`,eI=`vec3 transformed = vec3( position );
#ifdef USE_ALPHAHASH
	vPosition = vec3( position );
#endif`,tI=`vec3 objectNormal = vec3( normal );
#ifdef USE_TANGENT
	vec3 objectTangent = vec3( tangent.xyz );
#endif`,nI=`float G_BlinnPhong_Implicit( ) {
	return 0.25;
}
float D_BlinnPhong( const in float shininess, const in float dotNH ) {
	return RECIPROCAL_PI * ( shininess * 0.5 + 1.0 ) * pow( dotNH, shininess );
}
vec3 BRDF_BlinnPhong( const in vec3 lightDir, const in vec3 viewDir, const in vec3 normal, const in vec3 specularColor, const in float shininess ) {
	vec3 halfDir = normalize( lightDir + viewDir );
	float dotNH = saturate( dot( normal, halfDir ) );
	float dotVH = saturate( dot( viewDir, halfDir ) );
	vec3 F = F_Schlick( specularColor, 1.0, dotVH );
	float G = G_BlinnPhong_Implicit( );
	float D = D_BlinnPhong( shininess, dotNH );
	return F * ( G * D );
} // validated`,iI=`#ifdef USE_IRIDESCENCE
	const mat3 XYZ_TO_REC709 = mat3(
		 3.2404542, -0.9692660,  0.0556434,
		-1.5371385,  1.8760108, -0.2040259,
		-0.4985314,  0.0415560,  1.0572252
	);
	vec3 Fresnel0ToIor( vec3 fresnel0 ) {
		vec3 sqrtF0 = sqrt( fresnel0 );
		return ( vec3( 1.0 ) + sqrtF0 ) / ( vec3( 1.0 ) - sqrtF0 );
	}
	vec3 IorToFresnel0( vec3 transmittedIor, float incidentIor ) {
		return pow2( ( transmittedIor - vec3( incidentIor ) ) / ( transmittedIor + vec3( incidentIor ) ) );
	}
	float IorToFresnel0( float transmittedIor, float incidentIor ) {
		return pow2( ( transmittedIor - incidentIor ) / ( transmittedIor + incidentIor ));
	}
	vec3 evalSensitivity( float OPD, vec3 shift ) {
		float phase = 2.0 * PI * OPD * 1.0e-9;
		vec3 val = vec3( 5.4856e-13, 4.4201e-13, 5.2481e-13 );
		vec3 pos = vec3( 1.6810e+06, 1.7953e+06, 2.2084e+06 );
		vec3 var = vec3( 4.3278e+09, 9.3046e+09, 6.6121e+09 );
		vec3 xyz = val * sqrt( 2.0 * PI * var ) * cos( pos * phase + shift ) * exp( - pow2( phase ) * var );
		xyz.x += 9.7470e-14 * sqrt( 2.0 * PI * 4.5282e+09 ) * cos( 2.2399e+06 * phase + shift[ 0 ] ) * exp( - 4.5282e+09 * pow2( phase ) );
		xyz /= 1.0685e-7;
		vec3 rgb = XYZ_TO_REC709 * xyz;
		return rgb;
	}
	vec3 evalIridescence( float outsideIOR, float eta2, float cosTheta1, float thinFilmThickness, vec3 baseF0 ) {
		vec3 I;
		float iridescenceIOR = mix( outsideIOR, eta2, smoothstep( 0.0, 0.03, thinFilmThickness ) );
		float sinTheta2Sq = pow2( outsideIOR / iridescenceIOR ) * ( 1.0 - pow2( cosTheta1 ) );
		float cosTheta2Sq = 1.0 - sinTheta2Sq;
		if ( cosTheta2Sq < 0.0 ) {
			return vec3( 1.0 );
		}
		float cosTheta2 = sqrt( cosTheta2Sq );
		float R0 = IorToFresnel0( iridescenceIOR, outsideIOR );
		float R12 = F_Schlick( R0, 1.0, cosTheta1 );
		float T121 = 1.0 - R12;
		float phi12 = 0.0;
		if ( iridescenceIOR < outsideIOR ) phi12 = PI;
		float phi21 = PI - phi12;
		vec3 baseIOR = Fresnel0ToIor( clamp( baseF0, 0.0, 0.9999 ) );		vec3 R1 = IorToFresnel0( baseIOR, iridescenceIOR );
		vec3 R23 = F_Schlick( R1, 1.0, cosTheta2 );
		vec3 phi23 = vec3( 0.0 );
		if ( baseIOR[ 0 ] < iridescenceIOR ) phi23[ 0 ] = PI;
		if ( baseIOR[ 1 ] < iridescenceIOR ) phi23[ 1 ] = PI;
		if ( baseIOR[ 2 ] < iridescenceIOR ) phi23[ 2 ] = PI;
		float OPD = 2.0 * iridescenceIOR * thinFilmThickness * cosTheta2;
		vec3 phi = vec3( phi21 ) + phi23;
		vec3 R123 = clamp( R12 * R23, 1e-5, 0.9999 );
		vec3 r123 = sqrt( R123 );
		vec3 Rs = pow2( T121 ) * R23 / ( vec3( 1.0 ) - R123 );
		vec3 C0 = R12 + Rs;
		I = C0;
		vec3 Cm = Rs - T121;
		for ( int m = 1; m <= 2; ++ m ) {
			Cm *= r123;
			vec3 Sm = 2.0 * evalSensitivity( float( m ) * OPD, float( m ) * phi );
			I += Cm * Sm;
		}
		return max( I, vec3( 0.0 ) );
	}
#endif`,rI=`#ifdef USE_BUMPMAP
	uniform sampler2D bumpMap;
	uniform float bumpScale;
	vec2 dHdxy_fwd() {
		vec2 dSTdx = dFdx( vBumpMapUv );
		vec2 dSTdy = dFdy( vBumpMapUv );
		float Hll = bumpScale * texture2D( bumpMap, vBumpMapUv ).x;
		float dBx = bumpScale * texture2D( bumpMap, vBumpMapUv + dSTdx ).x - Hll;
		float dBy = bumpScale * texture2D( bumpMap, vBumpMapUv + dSTdy ).x - Hll;
		return vec2( dBx, dBy );
	}
	vec3 perturbNormalArb( vec3 surf_pos, vec3 surf_norm, vec2 dHdxy, float faceDirection ) {
		vec3 vSigmaX = normalize( dFdx( surf_pos.xyz ) );
		vec3 vSigmaY = normalize( dFdy( surf_pos.xyz ) );
		vec3 vN = surf_norm;
		vec3 R1 = cross( vSigmaY, vN );
		vec3 R2 = cross( vN, vSigmaX );
		float fDet = dot( vSigmaX, R1 ) * faceDirection;
		vec3 vGrad = sign( fDet ) * ( dHdxy.x * R1 + dHdxy.y * R2 );
		return normalize( abs( fDet ) * surf_norm - vGrad );
	}
#endif`,sI=`#if NUM_CLIPPING_PLANES > 0
	vec4 plane;
	#ifdef ALPHA_TO_COVERAGE
		float distanceToPlane, distanceGradient;
		float clipOpacity = 1.0;
		#pragma unroll_loop_start
		for ( int i = 0; i < UNION_CLIPPING_PLANES; i ++ ) {
			plane = clippingPlanes[ i ];
			distanceToPlane = - dot( vClipPosition, plane.xyz ) + plane.w;
			distanceGradient = fwidth( distanceToPlane ) / 2.0;
			clipOpacity *= smoothstep( - distanceGradient, distanceGradient, distanceToPlane );
			if ( clipOpacity == 0.0 ) discard;
		}
		#pragma unroll_loop_end
		#if UNION_CLIPPING_PLANES < NUM_CLIPPING_PLANES
			float unionClipOpacity = 1.0;
			#pragma unroll_loop_start
			for ( int i = UNION_CLIPPING_PLANES; i < NUM_CLIPPING_PLANES; i ++ ) {
				plane = clippingPlanes[ i ];
				distanceToPlane = - dot( vClipPosition, plane.xyz ) + plane.w;
				distanceGradient = fwidth( distanceToPlane ) / 2.0;
				unionClipOpacity *= 1.0 - smoothstep( - distanceGradient, distanceGradient, distanceToPlane );
			}
			#pragma unroll_loop_end
			clipOpacity *= 1.0 - unionClipOpacity;
		#endif
		diffuseColor.a *= clipOpacity;
		if ( diffuseColor.a == 0.0 ) discard;
	#else
		#pragma unroll_loop_start
		for ( int i = 0; i < UNION_CLIPPING_PLANES; i ++ ) {
			plane = clippingPlanes[ i ];
			if ( dot( vClipPosition, plane.xyz ) > plane.w ) discard;
		}
		#pragma unroll_loop_end
		#if UNION_CLIPPING_PLANES < NUM_CLIPPING_PLANES
			bool clipped = true;
			#pragma unroll_loop_start
			for ( int i = UNION_CLIPPING_PLANES; i < NUM_CLIPPING_PLANES; i ++ ) {
				plane = clippingPlanes[ i ];
				clipped = ( dot( vClipPosition, plane.xyz ) > plane.w ) && clipped;
			}
			#pragma unroll_loop_end
			if ( clipped ) discard;
		#endif
	#endif
#endif`,oI=`#if NUM_CLIPPING_PLANES > 0
	varying vec3 vClipPosition;
	uniform vec4 clippingPlanes[ NUM_CLIPPING_PLANES ];
#endif`,aI=`#if NUM_CLIPPING_PLANES > 0
	varying vec3 vClipPosition;
#endif`,cI=`#if NUM_CLIPPING_PLANES > 0
	vClipPosition = - mvPosition.xyz;
#endif`,lI=`#if defined( USE_COLOR ) || defined( USE_COLOR_ALPHA )
	diffuseColor *= vColor;
#endif`,uI=`#if defined( USE_COLOR ) || defined( USE_COLOR_ALPHA )
	varying vec4 vColor;
#endif`,dI=`#if defined( USE_COLOR ) || defined( USE_COLOR_ALPHA ) || defined( USE_INSTANCING_COLOR ) || defined( USE_BATCHING_COLOR )
	varying vec4 vColor;
#endif`,fI=`#if defined( USE_COLOR ) || defined( USE_COLOR_ALPHA ) || defined( USE_INSTANCING_COLOR ) || defined( USE_BATCHING_COLOR )
	vColor = vec4( 1.0 );
#endif
#ifdef USE_COLOR_ALPHA
	vColor *= color;
#elif defined( USE_COLOR )
	vColor.rgb *= color;
#endif
#ifdef USE_INSTANCING_COLOR
	vColor.rgb *= instanceColor.rgb;
#endif
#ifdef USE_BATCHING_COLOR
	vColor *= getBatchingColor( getIndirectIndex( gl_DrawID ) );
#endif`,hI=`#define PI 3.141592653589793
#define PI2 6.283185307179586
#define PI_HALF 1.5707963267948966
#define RECIPROCAL_PI 0.3183098861837907
#define RECIPROCAL_PI2 0.15915494309189535
#define EPSILON 1e-6
#ifndef saturate
#define saturate( a ) clamp( a, 0.0, 1.0 )
#endif
#define whiteComplement( a ) ( 1.0 - saturate( a ) )
float pow2( const in float x ) { return x*x; }
vec3 pow2( const in vec3 x ) { return x*x; }
float pow3( const in float x ) { return x*x*x; }
float pow4( const in float x ) { float x2 = x*x; return x2*x2; }
float max3( const in vec3 v ) { return max( max( v.x, v.y ), v.z ); }
float average( const in vec3 v ) { return dot( v, vec3( 0.3333333 ) ); }
highp float rand( const in vec2 uv ) {
	const highp float a = 12.9898, b = 78.233, c = 43758.5453;
	highp float dt = dot( uv.xy, vec2( a,b ) ), sn = mod( dt, PI );
	return fract( sin( sn ) * c );
}
#ifdef HIGH_PRECISION
	float precisionSafeLength( vec3 v ) { return length( v ); }
#else
	float precisionSafeLength( vec3 v ) {
		float maxComponent = max3( abs( v ) );
		return length( v / maxComponent ) * maxComponent;
	}
#endif
struct IncidentLight {
	vec3 color;
	vec3 direction;
	bool visible;
};
struct ReflectedLight {
	vec3 directDiffuse;
	vec3 directSpecular;
	vec3 indirectDiffuse;
	vec3 indirectSpecular;
};
#ifdef USE_ALPHAHASH
	varying vec3 vPosition;
#endif
vec3 transformDirection( in vec3 dir, in mat4 matrix ) {
	return normalize( ( matrix * vec4( dir, 0.0 ) ).xyz );
}
vec3 inverseTransformDirection( in vec3 dir, in mat4 matrix ) {
	return normalize( ( vec4( dir, 0.0 ) * matrix ).xyz );
}
bool isPerspectiveMatrix( mat4 m ) {
	return m[ 2 ][ 3 ] == - 1.0;
}
vec2 equirectUv( in vec3 dir ) {
	float u = atan( dir.z, dir.x ) * RECIPROCAL_PI2 + 0.5;
	float v = asin( clamp( dir.y, - 1.0, 1.0 ) ) * RECIPROCAL_PI + 0.5;
	return vec2( u, v );
}
vec3 BRDF_Lambert( const in vec3 diffuseColor ) {
	return RECIPROCAL_PI * diffuseColor;
}
vec3 F_Schlick( const in vec3 f0, const in float f90, const in float dotVH ) {
	float fresnel = exp2( ( - 5.55473 * dotVH - 6.98316 ) * dotVH );
	return f0 * ( 1.0 - fresnel ) + ( f90 * fresnel );
}
float F_Schlick( const in float f0, const in float f90, const in float dotVH ) {
	float fresnel = exp2( ( - 5.55473 * dotVH - 6.98316 ) * dotVH );
	return f0 * ( 1.0 - fresnel ) + ( f90 * fresnel );
} // validated`,pI=`#ifdef ENVMAP_TYPE_CUBE_UV
	#define cubeUV_minMipLevel 4.0
	#define cubeUV_minTileSize 16.0
	float getFace( vec3 direction ) {
		vec3 absDirection = abs( direction );
		float face = - 1.0;
		if ( absDirection.x > absDirection.z ) {
			if ( absDirection.x > absDirection.y )
				face = direction.x > 0.0 ? 0.0 : 3.0;
			else
				face = direction.y > 0.0 ? 1.0 : 4.0;
		} else {
			if ( absDirection.z > absDirection.y )
				face = direction.z > 0.0 ? 2.0 : 5.0;
			else
				face = direction.y > 0.0 ? 1.0 : 4.0;
		}
		return face;
	}
	vec2 getUV( vec3 direction, float face ) {
		vec2 uv;
		if ( face == 0.0 ) {
			uv = vec2( direction.z, direction.y ) / abs( direction.x );
		} else if ( face == 1.0 ) {
			uv = vec2( - direction.x, - direction.z ) / abs( direction.y );
		} else if ( face == 2.0 ) {
			uv = vec2( - direction.x, direction.y ) / abs( direction.z );
		} else if ( face == 3.0 ) {
			uv = vec2( - direction.z, direction.y ) / abs( direction.x );
		} else if ( face == 4.0 ) {
			uv = vec2( - direction.x, direction.z ) / abs( direction.y );
		} else {
			uv = vec2( direction.x, direction.y ) / abs( direction.z );
		}
		return 0.5 * ( uv + 1.0 );
	}
	vec3 bilinearCubeUV( sampler2D envMap, vec3 direction, float mipInt ) {
		float face = getFace( direction );
		float filterInt = max( cubeUV_minMipLevel - mipInt, 0.0 );
		mipInt = max( mipInt, cubeUV_minMipLevel );
		float faceSize = exp2( mipInt );
		highp vec2 uv = getUV( direction, face ) * ( faceSize - 2.0 ) + 1.0;
		if ( face > 2.0 ) {
			uv.y += faceSize;
			face -= 3.0;
		}
		uv.x += face * faceSize;
		uv.x += filterInt * 3.0 * cubeUV_minTileSize;
		uv.y += 4.0 * ( exp2( CUBEUV_MAX_MIP ) - faceSize );
		uv.x *= CUBEUV_TEXEL_WIDTH;
		uv.y *= CUBEUV_TEXEL_HEIGHT;
		#ifdef texture2DGradEXT
			return texture2DGradEXT( envMap, uv, vec2( 0.0 ), vec2( 0.0 ) ).rgb;
		#else
			return texture2D( envMap, uv ).rgb;
		#endif
	}
	#define cubeUV_r0 1.0
	#define cubeUV_m0 - 2.0
	#define cubeUV_r1 0.8
	#define cubeUV_m1 - 1.0
	#define cubeUV_r4 0.4
	#define cubeUV_m4 2.0
	#define cubeUV_r5 0.305
	#define cubeUV_m5 3.0
	#define cubeUV_r6 0.21
	#define cubeUV_m6 4.0
	float roughnessToMip( float roughness ) {
		float mip = 0.0;
		if ( roughness >= cubeUV_r1 ) {
			mip = ( cubeUV_r0 - roughness ) * ( cubeUV_m1 - cubeUV_m0 ) / ( cubeUV_r0 - cubeUV_r1 ) + cubeUV_m0;
		} else if ( roughness >= cubeUV_r4 ) {
			mip = ( cubeUV_r1 - roughness ) * ( cubeUV_m4 - cubeUV_m1 ) / ( cubeUV_r1 - cubeUV_r4 ) + cubeUV_m1;
		} else if ( roughness >= cubeUV_r5 ) {
			mip = ( cubeUV_r4 - roughness ) * ( cubeUV_m5 - cubeUV_m4 ) / ( cubeUV_r4 - cubeUV_r5 ) + cubeUV_m4;
		} else if ( roughness >= cubeUV_r6 ) {
			mip = ( cubeUV_r5 - roughness ) * ( cubeUV_m6 - cubeUV_m5 ) / ( cubeUV_r5 - cubeUV_r6 ) + cubeUV_m5;
		} else {
			mip = - 2.0 * log2( 1.16 * roughness );		}
		return mip;
	}
	vec4 textureCubeUV( sampler2D envMap, vec3 sampleDir, float roughness ) {
		float mip = clamp( roughnessToMip( roughness ), cubeUV_m0, CUBEUV_MAX_MIP );
		float mipF = fract( mip );
		float mipInt = floor( mip );
		vec3 color0 = bilinearCubeUV( envMap, sampleDir, mipInt );
		if ( mipF == 0.0 ) {
			return vec4( color0, 1.0 );
		} else {
			vec3 color1 = bilinearCubeUV( envMap, sampleDir, mipInt + 1.0 );
			return vec4( mix( color0, color1, mipF ), 1.0 );
		}
	}
#endif`,mI=`vec3 transformedNormal = objectNormal;
#ifdef USE_TANGENT
	vec3 transformedTangent = objectTangent;
#endif
#ifdef USE_BATCHING
	mat3 bm = mat3( batchingMatrix );
	transformedNormal /= vec3( dot( bm[ 0 ], bm[ 0 ] ), dot( bm[ 1 ], bm[ 1 ] ), dot( bm[ 2 ], bm[ 2 ] ) );
	transformedNormal = bm * transformedNormal;
	#ifdef USE_TANGENT
		transformedTangent = bm * transformedTangent;
	#endif
#endif
#ifdef USE_INSTANCING
	mat3 im = mat3( instanceMatrix );
	transformedNormal /= vec3( dot( im[ 0 ], im[ 0 ] ), dot( im[ 1 ], im[ 1 ] ), dot( im[ 2 ], im[ 2 ] ) );
	transformedNormal = im * transformedNormal;
	#ifdef USE_TANGENT
		transformedTangent = im * transformedTangent;
	#endif
#endif
transformedNormal = normalMatrix * transformedNormal;
#ifdef FLIP_SIDED
	transformedNormal = - transformedNormal;
#endif
#ifdef USE_TANGENT
	transformedTangent = ( modelViewMatrix * vec4( transformedTangent, 0.0 ) ).xyz;
	#ifdef FLIP_SIDED
		transformedTangent = - transformedTangent;
	#endif
#endif`,gI=`#ifdef USE_DISPLACEMENTMAP
	uniform sampler2D displacementMap;
	uniform float displacementScale;
	uniform float displacementBias;
#endif`,vI=`#ifdef USE_DISPLACEMENTMAP
	transformed += normalize( objectNormal ) * ( texture2D( displacementMap, vDisplacementMapUv ).x * displacementScale + displacementBias );
#endif`,yI=`#ifdef USE_EMISSIVEMAP
	vec4 emissiveColor = texture2D( emissiveMap, vEmissiveMapUv );
	#ifdef DECODE_VIDEO_TEXTURE_EMISSIVE
		emissiveColor = sRGBTransferEOTF( emissiveColor );
	#endif
	totalEmissiveRadiance *= emissiveColor.rgb;
#endif`,_I=`#ifdef USE_EMISSIVEMAP
	uniform sampler2D emissiveMap;
#endif`,xI="gl_FragColor = linearToOutputTexel( gl_FragColor );",MI=`vec4 LinearTransferOETF( in vec4 value ) {
	return value;
}
vec4 sRGBTransferEOTF( in vec4 value ) {
	return vec4( mix( pow( value.rgb * 0.9478672986 + vec3( 0.0521327014 ), vec3( 2.4 ) ), value.rgb * 0.0773993808, vec3( lessThanEqual( value.rgb, vec3( 0.04045 ) ) ) ), value.a );
}
vec4 sRGBTransferOETF( in vec4 value ) {
	return vec4( mix( pow( value.rgb, vec3( 0.41666 ) ) * 1.055 - vec3( 0.055 ), value.rgb * 12.92, vec3( lessThanEqual( value.rgb, vec3( 0.0031308 ) ) ) ), value.a );
}`,bI=`#ifdef USE_ENVMAP
	#ifdef ENV_WORLDPOS
		vec3 cameraToFrag;
		if ( isOrthographic ) {
			cameraToFrag = normalize( vec3( - viewMatrix[ 0 ][ 2 ], - viewMatrix[ 1 ][ 2 ], - viewMatrix[ 2 ][ 2 ] ) );
		} else {
			cameraToFrag = normalize( vWorldPosition - cameraPosition );
		}
		vec3 worldNormal = inverseTransformDirection( normal, viewMatrix );
		#ifdef ENVMAP_MODE_REFLECTION
			vec3 reflectVec = reflect( cameraToFrag, worldNormal );
		#else
			vec3 reflectVec = refract( cameraToFrag, worldNormal, refractionRatio );
		#endif
	#else
		vec3 reflectVec = vReflect;
	#endif
	#ifdef ENVMAP_TYPE_CUBE
		vec4 envColor = textureCube( envMap, envMapRotation * vec3( flipEnvMap * reflectVec.x, reflectVec.yz ) );
		#ifdef ENVMAP_BLENDING_MULTIPLY
			outgoingLight = mix( outgoingLight, outgoingLight * envColor.xyz, specularStrength * reflectivity );
		#elif defined( ENVMAP_BLENDING_MIX )
			outgoingLight = mix( outgoingLight, envColor.xyz, specularStrength * reflectivity );
		#elif defined( ENVMAP_BLENDING_ADD )
			outgoingLight += envColor.xyz * specularStrength * reflectivity;
		#endif
	#endif
#endif`,SI=`#ifdef USE_ENVMAP
	uniform float envMapIntensity;
	uniform float flipEnvMap;
	uniform mat3 envMapRotation;
	#ifdef ENVMAP_TYPE_CUBE
		uniform samplerCube envMap;
	#else
		uniform sampler2D envMap;
	#endif
#endif`,EI=`#ifdef USE_ENVMAP
	uniform float reflectivity;
	#if defined( USE_BUMPMAP ) || defined( USE_NORMALMAP ) || defined( PHONG ) || defined( LAMBERT )
		#define ENV_WORLDPOS
	#endif
	#ifdef ENV_WORLDPOS
		varying vec3 vWorldPosition;
		uniform float refractionRatio;
	#else
		varying vec3 vReflect;
	#endif
#endif`,TI=`#ifdef USE_ENVMAP
	#if defined( USE_BUMPMAP ) || defined( USE_NORMALMAP ) || defined( PHONG ) || defined( LAMBERT )
		#define ENV_WORLDPOS
	#endif
	#ifdef ENV_WORLDPOS
		
		varying vec3 vWorldPosition;
	#else
		varying vec3 vReflect;
		uniform float refractionRatio;
	#endif
#endif`,wI=`#ifdef USE_ENVMAP
	#ifdef ENV_WORLDPOS
		vWorldPosition = worldPosition.xyz;
	#else
		vec3 cameraToVertex;
		if ( isOrthographic ) {
			cameraToVertex = normalize( vec3( - viewMatrix[ 0 ][ 2 ], - viewMatrix[ 1 ][ 2 ], - viewMatrix[ 2 ][ 2 ] ) );
		} else {
			cameraToVertex = normalize( worldPosition.xyz - cameraPosition );
		}
		vec3 worldNormal = inverseTransformDirection( transformedNormal, viewMatrix );
		#ifdef ENVMAP_MODE_REFLECTION
			vReflect = reflect( cameraToVertex, worldNormal );
		#else
			vReflect = refract( cameraToVertex, worldNormal, refractionRatio );
		#endif
	#endif
#endif`,CI=`#ifdef USE_FOG
	vFogDepth = - mvPosition.z;
#endif`,II=`#ifdef USE_FOG
	varying float vFogDepth;
#endif`,AI=`#ifdef USE_FOG
	#ifdef FOG_EXP2
		float fogFactor = 1.0 - exp( - fogDensity * fogDensity * vFogDepth * vFogDepth );
	#else
		float fogFactor = smoothstep( fogNear, fogFar, vFogDepth );
	#endif
	gl_FragColor.rgb = mix( gl_FragColor.rgb, fogColor, fogFactor );
#endif`,DI=`#ifdef USE_FOG
	uniform vec3 fogColor;
	varying float vFogDepth;
	#ifdef FOG_EXP2
		uniform float fogDensity;
	#else
		uniform float fogNear;
		uniform float fogFar;
	#endif
#endif`,RI=`#ifdef USE_GRADIENTMAP
	uniform sampler2D gradientMap;
#endif
vec3 getGradientIrradiance( vec3 normal, vec3 lightDirection ) {
	float dotNL = dot( normal, lightDirection );
	vec2 coord = vec2( dotNL * 0.5 + 0.5, 0.0 );
	#ifdef USE_GRADIENTMAP
		return vec3( texture2D( gradientMap, coord ).r );
	#else
		vec2 fw = fwidth( coord ) * 0.5;
		return mix( vec3( 0.7 ), vec3( 1.0 ), smoothstep( 0.7 - fw.x, 0.7 + fw.x, coord.x ) );
	#endif
}`,NI=`#ifdef USE_LIGHTMAP
	uniform sampler2D lightMap;
	uniform float lightMapIntensity;
#endif`,PI=`LambertMaterial material;
material.diffuseColor = diffuseColor.rgb;
material.specularStrength = specularStrength;`,LI=`varying vec3 vViewPosition;
struct LambertMaterial {
	vec3 diffuseColor;
	float specularStrength;
};
void RE_Direct_Lambert( const in IncidentLight directLight, const in vec3 geometryPosition, const in vec3 geometryNormal, const in vec3 geometryViewDir, const in vec3 geometryClearcoatNormal, const in LambertMaterial material, inout ReflectedLight reflectedLight ) {
	float dotNL = saturate( dot( geometryNormal, directLight.direction ) );
	vec3 irradiance = dotNL * directLight.color;
	reflectedLight.directDiffuse += irradiance * BRDF_Lambert( material.diffuseColor );
}
void RE_IndirectDiffuse_Lambert( const in vec3 irradiance, const in vec3 geometryPosition, const in vec3 geometryNormal, const in vec3 geometryViewDir, const in vec3 geometryClearcoatNormal, const in LambertMaterial material, inout ReflectedLight reflectedLight ) {
	reflectedLight.indirectDiffuse += irradiance * BRDF_Lambert( material.diffuseColor );
}
#define RE_Direct				RE_Direct_Lambert
#define RE_IndirectDiffuse		RE_IndirectDiffuse_Lambert`,OI=`uniform bool receiveShadow;
uniform vec3 ambientLightColor;
#if defined( USE_LIGHT_PROBES )
	uniform vec3 lightProbe[ 9 ];
#endif
vec3 shGetIrradianceAt( in vec3 normal, in vec3 shCoefficients[ 9 ] ) {
	float x = normal.x, y = normal.y, z = normal.z;
	vec3 result = shCoefficients[ 0 ] * 0.886227;
	result += shCoefficients[ 1 ] * 2.0 * 0.511664 * y;
	result += shCoefficients[ 2 ] * 2.0 * 0.511664 * z;
	result += shCoefficients[ 3 ] * 2.0 * 0.511664 * x;
	result += shCoefficients[ 4 ] * 2.0 * 0.429043 * x * y;
	result += shCoefficients[ 5 ] * 2.0 * 0.429043 * y * z;
	result += shCoefficients[ 6 ] * ( 0.743125 * z * z - 0.247708 );
	result += shCoefficients[ 7 ] * 2.0 * 0.429043 * x * z;
	result += shCoefficients[ 8 ] * 0.429043 * ( x * x - y * y );
	return result;
}
vec3 getLightProbeIrradiance( const in vec3 lightProbe[ 9 ], const in vec3 normal ) {
	vec3 worldNormal = inverseTransformDirection( normal, viewMatrix );
	vec3 irradiance = shGetIrradianceAt( worldNormal, lightProbe );
	return irradiance;
}
vec3 getAmbientLightIrradiance( const in vec3 ambientLightColor ) {
	vec3 irradiance = ambientLightColor;
	return irradiance;
}
float getDistanceAttenuation( const in float lightDistance, const in float cutoffDistance, const in float decayExponent ) {
	float distanceFalloff = 1.0 / max( pow( lightDistance, decayExponent ), 0.01 );
	if ( cutoffDistance > 0.0 ) {
		distanceFalloff *= pow2( saturate( 1.0 - pow4( lightDistance / cutoffDistance ) ) );
	}
	return distanceFalloff;
}
float getSpotAttenuation( const in float coneCosine, const in float penumbraCosine, const in float angleCosine ) {
	return smoothstep( coneCosine, penumbraCosine, angleCosine );
}
#if NUM_DIR_LIGHTS > 0
	struct DirectionalLight {
		vec3 direction;
		vec3 color;
	};
	uniform DirectionalLight directionalLights[ NUM_DIR_LIGHTS ];
	void getDirectionalLightInfo( const in DirectionalLight directionalLight, out IncidentLight light ) {
		light.color = directionalLight.color;
		light.direction = directionalLight.direction;
		light.visible = true;
	}
#endif
#if NUM_POINT_LIGHTS > 0
	struct PointLight {
		vec3 position;
		vec3 color;
		float distance;
		float decay;
	};
	uniform PointLight pointLights[ NUM_POINT_LIGHTS ];
	void getPointLightInfo( const in PointLight pointLight, const in vec3 geometryPosition, out IncidentLight light ) {
		vec3 lVector = pointLight.position - geometryPosition;
		light.direction = normalize( lVector );
		float lightDistance = length( lVector );
		light.color = pointLight.color;
		light.color *= getDistanceAttenuation( lightDistance, pointLight.distance, pointLight.decay );
		light.visible = ( light.color != vec3( 0.0 ) );
	}
#endif
#if NUM_SPOT_LIGHTS > 0
	struct SpotLight {
		vec3 position;
		vec3 direction;
		vec3 color;
		float distance;
		float decay;
		float coneCos;
		float penumbraCos;
	};
	uniform SpotLight spotLights[ NUM_SPOT_LIGHTS ];
	void getSpotLightInfo( const in SpotLight spotLight, const in vec3 geometryPosition, out IncidentLight light ) {
		vec3 lVector = spotLight.position - geometryPosition;
		light.direction = normalize( lVector );
		float angleCos = dot( light.direction, spotLight.direction );
		float spotAttenuation = getSpotAttenuation( spotLight.coneCos, spotLight.penumbraCos, angleCos );
		if ( spotAttenuation > 0.0 ) {
			float lightDistance = length( lVector );
			light.color = spotLight.color * spotAttenuation;
			light.color *= getDistanceAttenuation( lightDistance, spotLight.distance, spotLight.decay );
			light.visible = ( light.color != vec3( 0.0 ) );
		} else {
			light.color = vec3( 0.0 );
			light.visible = false;
		}
	}
#endif
#if NUM_RECT_AREA_LIGHTS > 0
	struct RectAreaLight {
		vec3 color;
		vec3 position;
		vec3 halfWidth;
		vec3 halfHeight;
	};
	uniform sampler2D ltc_1;	uniform sampler2D ltc_2;
	uniform RectAreaLight rectAreaLights[ NUM_RECT_AREA_LIGHTS ];
#endif
#if NUM_HEMI_LIGHTS > 0
	struct HemisphereLight {
		vec3 direction;
		vec3 skyColor;
		vec3 groundColor;
	};
	uniform HemisphereLight hemisphereLights[ NUM_HEMI_LIGHTS ];
	vec3 getHemisphereLightIrradiance( const in HemisphereLight hemiLight, const in vec3 normal ) {
		float dotNL = dot( normal, hemiLight.direction );
		float hemiDiffuseWeight = 0.5 * dotNL + 0.5;
		vec3 irradiance = mix( hemiLight.groundColor, hemiLight.skyColor, hemiDiffuseWeight );
		return irradiance;
	}
#endif`,FI=`#ifdef USE_ENVMAP
	vec3 getIBLIrradiance( const in vec3 normal ) {
		#ifdef ENVMAP_TYPE_CUBE_UV
			vec3 worldNormal = inverseTransformDirection( normal, viewMatrix );
			vec4 envMapColor = textureCubeUV( envMap, envMapRotation * worldNormal, 1.0 );
			return PI * envMapColor.rgb * envMapIntensity;
		#else
			return vec3( 0.0 );
		#endif
	}
	vec3 getIBLRadiance( const in vec3 viewDir, const in vec3 normal, const in float roughness ) {
		#ifdef ENVMAP_TYPE_CUBE_UV
			vec3 reflectVec = reflect( - viewDir, normal );
			reflectVec = normalize( mix( reflectVec, normal, pow4( roughness ) ) );
			reflectVec = inverseTransformDirection( reflectVec, viewMatrix );
			vec4 envMapColor = textureCubeUV( envMap, envMapRotation * reflectVec, roughness );
			return envMapColor.rgb * envMapIntensity;
		#else
			return vec3( 0.0 );
		#endif
	}
	#ifdef USE_ANISOTROPY
		vec3 getIBLAnisotropyRadiance( const in vec3 viewDir, const in vec3 normal, const in float roughness, const in vec3 bitangent, const in float anisotropy ) {
			#ifdef ENVMAP_TYPE_CUBE_UV
				vec3 bentNormal = cross( bitangent, viewDir );
				bentNormal = normalize( cross( bentNormal, bitangent ) );
				bentNormal = normalize( mix( bentNormal, normal, pow2( pow2( 1.0 - anisotropy * ( 1.0 - roughness ) ) ) ) );
				return getIBLRadiance( viewDir, bentNormal, roughness );
			#else
				return vec3( 0.0 );
			#endif
		}
	#endif
#endif`,kI=`ToonMaterial material;
material.diffuseColor = diffuseColor.rgb;`,UI=`varying vec3 vViewPosition;
struct ToonMaterial {
	vec3 diffuseColor;
};
void RE_Direct_Toon( const in IncidentLight directLight, const in vec3 geometryPosition, const in vec3 geometryNormal, const in vec3 geometryViewDir, const in vec3 geometryClearcoatNormal, const in ToonMaterial material, inout ReflectedLight reflectedLight ) {
	vec3 irradiance = getGradientIrradiance( geometryNormal, directLight.direction ) * directLight.color;
	reflectedLight.directDiffuse += irradiance * BRDF_Lambert( material.diffuseColor );
}
void RE_IndirectDiffuse_Toon( const in vec3 irradiance, const in vec3 geometryPosition, const in vec3 geometryNormal, const in vec3 geometryViewDir, const in vec3 geometryClearcoatNormal, const in ToonMaterial material, inout ReflectedLight reflectedLight ) {
	reflectedLight.indirectDiffuse += irradiance * BRDF_Lambert( material.diffuseColor );
}
#define RE_Direct				RE_Direct_Toon
#define RE_IndirectDiffuse		RE_IndirectDiffuse_Toon`,BI=`BlinnPhongMaterial material;
material.diffuseColor = diffuseColor.rgb;
material.specularColor = specular;
material.specularShininess = shininess;
material.specularStrength = specularStrength;`,VI=`varying vec3 vViewPosition;
struct BlinnPhongMaterial {
	vec3 diffuseColor;
	vec3 specularColor;
	float specularShininess;
	float specularStrength;
};
void RE_Direct_BlinnPhong( const in IncidentLight directLight, const in vec3 geometryPosition, const in vec3 geometryNormal, const in vec3 geometryViewDir, const in vec3 geometryClearcoatNormal, const in BlinnPhongMaterial material, inout ReflectedLight reflectedLight ) {
	float dotNL = saturate( dot( geometryNormal, directLight.direction ) );
	vec3 irradiance = dotNL * directLight.color;
	reflectedLight.directDiffuse += irradiance * BRDF_Lambert( material.diffuseColor );
	reflectedLight.directSpecular += irradiance * BRDF_BlinnPhong( directLight.direction, geometryViewDir, geometryNormal, material.specularColor, material.specularShininess ) * material.specularStrength;
}
void RE_IndirectDiffuse_BlinnPhong( const in vec3 irradiance, const in vec3 geometryPosition, const in vec3 geometryNormal, const in vec3 geometryViewDir, const in vec3 geometryClearcoatNormal, const in BlinnPhongMaterial material, inout ReflectedLight reflectedLight ) {
	reflectedLight.indirectDiffuse += irradiance * BRDF_Lambert( material.diffuseColor );
}
#define RE_Direct				RE_Direct_BlinnPhong
#define RE_IndirectDiffuse		RE_IndirectDiffuse_BlinnPhong`,HI=`PhysicalMaterial material;
material.diffuseColor = diffuseColor.rgb;
material.diffuseContribution = diffuseColor.rgb * ( 1.0 - metalnessFactor );
material.metalness = metalnessFactor;
vec3 dxy = max( abs( dFdx( nonPerturbedNormal ) ), abs( dFdy( nonPerturbedNormal ) ) );
float geometryRoughness = max( max( dxy.x, dxy.y ), dxy.z );
material.roughness = max( roughnessFactor, 0.0525 );material.roughness += geometryRoughness;
material.roughness = min( material.roughness, 1.0 );
#ifdef IOR
	material.ior = ior;
	#ifdef USE_SPECULAR
		float specularIntensityFactor = specularIntensity;
		vec3 specularColorFactor = specularColor;
		#ifdef USE_SPECULAR_COLORMAP
			specularColorFactor *= texture2D( specularColorMap, vSpecularColorMapUv ).rgb;
		#endif
		#ifdef USE_SPECULAR_INTENSITYMAP
			specularIntensityFactor *= texture2D( specularIntensityMap, vSpecularIntensityMapUv ).a;
		#endif
		material.specularF90 = mix( specularIntensityFactor, 1.0, metalnessFactor );
	#else
		float specularIntensityFactor = 1.0;
		vec3 specularColorFactor = vec3( 1.0 );
		material.specularF90 = 1.0;
	#endif
	material.specularColor = min( pow2( ( material.ior - 1.0 ) / ( material.ior + 1.0 ) ) * specularColorFactor, vec3( 1.0 ) ) * specularIntensityFactor;
	material.specularColorBlended = mix( material.specularColor, diffuseColor.rgb, metalnessFactor );
#else
	material.specularColor = vec3( 0.04 );
	material.specularColorBlended = mix( material.specularColor, diffuseColor.rgb, metalnessFactor );
	material.specularF90 = 1.0;
#endif
#ifdef USE_CLEARCOAT
	material.clearcoat = clearcoat;
	material.clearcoatRoughness = clearcoatRoughness;
	material.clearcoatF0 = vec3( 0.04 );
	material.clearcoatF90 = 1.0;
	#ifdef USE_CLEARCOATMAP
		material.clearcoat *= texture2D( clearcoatMap, vClearcoatMapUv ).x;
	#endif
	#ifdef USE_CLEARCOAT_ROUGHNESSMAP
		material.clearcoatRoughness *= texture2D( clearcoatRoughnessMap, vClearcoatRoughnessMapUv ).y;
	#endif
	material.clearcoat = saturate( material.clearcoat );	material.clearcoatRoughness = max( material.clearcoatRoughness, 0.0525 );
	material.clearcoatRoughness += geometryRoughness;
	material.clearcoatRoughness = min( material.clearcoatRoughness, 1.0 );
#endif
#ifdef USE_DISPERSION
	material.dispersion = dispersion;
#endif
#ifdef USE_IRIDESCENCE
	material.iridescence = iridescence;
	material.iridescenceIOR = iridescenceIOR;
	#ifdef USE_IRIDESCENCEMAP
		material.iridescence *= texture2D( iridescenceMap, vIridescenceMapUv ).r;
	#endif
	#ifdef USE_IRIDESCENCE_THICKNESSMAP
		material.iridescenceThickness = (iridescenceThicknessMaximum - iridescenceThicknessMinimum) * texture2D( iridescenceThicknessMap, vIridescenceThicknessMapUv ).g + iridescenceThicknessMinimum;
	#else
		material.iridescenceThickness = iridescenceThicknessMaximum;
	#endif
#endif
#ifdef USE_SHEEN
	material.sheenColor = sheenColor;
	#ifdef USE_SHEEN_COLORMAP
		material.sheenColor *= texture2D( sheenColorMap, vSheenColorMapUv ).rgb;
	#endif
	material.sheenRoughness = clamp( sheenRoughness, 0.0001, 1.0 );
	#ifdef USE_SHEEN_ROUGHNESSMAP
		material.sheenRoughness *= texture2D( sheenRoughnessMap, vSheenRoughnessMapUv ).a;
	#endif
#endif
#ifdef USE_ANISOTROPY
	#ifdef USE_ANISOTROPYMAP
		mat2 anisotropyMat = mat2( anisotropyVector.x, anisotropyVector.y, - anisotropyVector.y, anisotropyVector.x );
		vec3 anisotropyPolar = texture2D( anisotropyMap, vAnisotropyMapUv ).rgb;
		vec2 anisotropyV = anisotropyMat * normalize( 2.0 * anisotropyPolar.rg - vec2( 1.0 ) ) * anisotropyPolar.b;
	#else
		vec2 anisotropyV = anisotropyVector;
	#endif
	material.anisotropy = length( anisotropyV );
	if( material.anisotropy == 0.0 ) {
		anisotropyV = vec2( 1.0, 0.0 );
	} else {
		anisotropyV /= material.anisotropy;
		material.anisotropy = saturate( material.anisotropy );
	}
	material.alphaT = mix( pow2( material.roughness ), 1.0, pow2( material.anisotropy ) );
	material.anisotropyT = tbn[ 0 ] * anisotropyV.x + tbn[ 1 ] * anisotropyV.y;
	material.anisotropyB = tbn[ 1 ] * anisotropyV.x - tbn[ 0 ] * anisotropyV.y;
#endif`,zI=`uniform sampler2D dfgLUT;
struct PhysicalMaterial {
	vec3 diffuseColor;
	vec3 diffuseContribution;
	vec3 specularColor;
	vec3 specularColorBlended;
	float roughness;
	float metalness;
	float specularF90;
	float dispersion;
	#ifdef USE_CLEARCOAT
		float clearcoat;
		float clearcoatRoughness;
		vec3 clearcoatF0;
		float clearcoatF90;
	#endif
	#ifdef USE_IRIDESCENCE
		float iridescence;
		float iridescenceIOR;
		float iridescenceThickness;
		vec3 iridescenceFresnel;
		vec3 iridescenceF0;
		vec3 iridescenceFresnelDielectric;
		vec3 iridescenceFresnelMetallic;
	#endif
	#ifdef USE_SHEEN
		vec3 sheenColor;
		float sheenRoughness;
	#endif
	#ifdef IOR
		float ior;
	#endif
	#ifdef USE_TRANSMISSION
		float transmission;
		float transmissionAlpha;
		float thickness;
		float attenuationDistance;
		vec3 attenuationColor;
	#endif
	#ifdef USE_ANISOTROPY
		float anisotropy;
		float alphaT;
		vec3 anisotropyT;
		vec3 anisotropyB;
	#endif
};
vec3 clearcoatSpecularDirect = vec3( 0.0 );
vec3 clearcoatSpecularIndirect = vec3( 0.0 );
vec3 sheenSpecularDirect = vec3( 0.0 );
vec3 sheenSpecularIndirect = vec3(0.0 );
vec3 Schlick_to_F0( const in vec3 f, const in float f90, const in float dotVH ) {
    float x = clamp( 1.0 - dotVH, 0.0, 1.0 );
    float x2 = x * x;
    float x5 = clamp( x * x2 * x2, 0.0, 0.9999 );
    return ( f - vec3( f90 ) * x5 ) / ( 1.0 - x5 );
}
float V_GGX_SmithCorrelated( const in float alpha, const in float dotNL, const in float dotNV ) {
	float a2 = pow2( alpha );
	float gv = dotNL * sqrt( a2 + ( 1.0 - a2 ) * pow2( dotNV ) );
	float gl = dotNV * sqrt( a2 + ( 1.0 - a2 ) * pow2( dotNL ) );
	return 0.5 / max( gv + gl, EPSILON );
}
float D_GGX( const in float alpha, const in float dotNH ) {
	float a2 = pow2( alpha );
	float denom = pow2( dotNH ) * ( a2 - 1.0 ) + 1.0;
	return RECIPROCAL_PI * a2 / pow2( denom );
}
#ifdef USE_ANISOTROPY
	float V_GGX_SmithCorrelated_Anisotropic( const in float alphaT, const in float alphaB, const in float dotTV, const in float dotBV, const in float dotTL, const in float dotBL, const in float dotNV, const in float dotNL ) {
		float gv = dotNL * length( vec3( alphaT * dotTV, alphaB * dotBV, dotNV ) );
		float gl = dotNV * length( vec3( alphaT * dotTL, alphaB * dotBL, dotNL ) );
		float v = 0.5 / ( gv + gl );
		return v;
	}
	float D_GGX_Anisotropic( const in float alphaT, const in float alphaB, const in float dotNH, const in float dotTH, const in float dotBH ) {
		float a2 = alphaT * alphaB;
		highp vec3 v = vec3( alphaB * dotTH, alphaT * dotBH, a2 * dotNH );
		highp float v2 = dot( v, v );
		float w2 = a2 / v2;
		return RECIPROCAL_PI * a2 * pow2 ( w2 );
	}
#endif
#ifdef USE_CLEARCOAT
	vec3 BRDF_GGX_Clearcoat( const in vec3 lightDir, const in vec3 viewDir, const in vec3 normal, const in PhysicalMaterial material) {
		vec3 f0 = material.clearcoatF0;
		float f90 = material.clearcoatF90;
		float roughness = material.clearcoatRoughness;
		float alpha = pow2( roughness );
		vec3 halfDir = normalize( lightDir + viewDir );
		float dotNL = saturate( dot( normal, lightDir ) );
		float dotNV = saturate( dot( normal, viewDir ) );
		float dotNH = saturate( dot( normal, halfDir ) );
		float dotVH = saturate( dot( viewDir, halfDir ) );
		vec3 F = F_Schlick( f0, f90, dotVH );
		float V = V_GGX_SmithCorrelated( alpha, dotNL, dotNV );
		float D = D_GGX( alpha, dotNH );
		return F * ( V * D );
	}
#endif
vec3 BRDF_GGX( const in vec3 lightDir, const in vec3 viewDir, const in vec3 normal, const in PhysicalMaterial material ) {
	vec3 f0 = material.specularColorBlended;
	float f90 = material.specularF90;
	float roughness = material.roughness;
	float alpha = pow2( roughness );
	vec3 halfDir = normalize( lightDir + viewDir );
	float dotNL = saturate( dot( normal, lightDir ) );
	float dotNV = saturate( dot( normal, viewDir ) );
	float dotNH = saturate( dot( normal, halfDir ) );
	float dotVH = saturate( dot( viewDir, halfDir ) );
	vec3 F = F_Schlick( f0, f90, dotVH );
	#ifdef USE_IRIDESCENCE
		F = mix( F, material.iridescenceFresnel, material.iridescence );
	#endif
	#ifdef USE_ANISOTROPY
		float dotTL = dot( material.anisotropyT, lightDir );
		float dotTV = dot( material.anisotropyT, viewDir );
		float dotTH = dot( material.anisotropyT, halfDir );
		float dotBL = dot( material.anisotropyB, lightDir );
		float dotBV = dot( material.anisotropyB, viewDir );
		float dotBH = dot( material.anisotropyB, halfDir );
		float V = V_GGX_SmithCorrelated_Anisotropic( material.alphaT, alpha, dotTV, dotBV, dotTL, dotBL, dotNV, dotNL );
		float D = D_GGX_Anisotropic( material.alphaT, alpha, dotNH, dotTH, dotBH );
	#else
		float V = V_GGX_SmithCorrelated( alpha, dotNL, dotNV );
		float D = D_GGX( alpha, dotNH );
	#endif
	return F * ( V * D );
}
vec2 LTC_Uv( const in vec3 N, const in vec3 V, const in float roughness ) {
	const float LUT_SIZE = 64.0;
	const float LUT_SCALE = ( LUT_SIZE - 1.0 ) / LUT_SIZE;
	const float LUT_BIAS = 0.5 / LUT_SIZE;
	float dotNV = saturate( dot( N, V ) );
	vec2 uv = vec2( roughness, sqrt( 1.0 - dotNV ) );
	uv = uv * LUT_SCALE + LUT_BIAS;
	return uv;
}
float LTC_ClippedSphereFormFactor( const in vec3 f ) {
	float l = length( f );
	return max( ( l * l + f.z ) / ( l + 1.0 ), 0.0 );
}
vec3 LTC_EdgeVectorFormFactor( const in vec3 v1, const in vec3 v2 ) {
	float x = dot( v1, v2 );
	float y = abs( x );
	float a = 0.8543985 + ( 0.4965155 + 0.0145206 * y ) * y;
	float b = 3.4175940 + ( 4.1616724 + y ) * y;
	float v = a / b;
	float theta_sintheta = ( x > 0.0 ) ? v : 0.5 * inversesqrt( max( 1.0 - x * x, 1e-7 ) ) - v;
	return cross( v1, v2 ) * theta_sintheta;
}
vec3 LTC_Evaluate( const in vec3 N, const in vec3 V, const in vec3 P, const in mat3 mInv, const in vec3 rectCoords[ 4 ] ) {
	vec3 v1 = rectCoords[ 1 ] - rectCoords[ 0 ];
	vec3 v2 = rectCoords[ 3 ] - rectCoords[ 0 ];
	vec3 lightNormal = cross( v1, v2 );
	if( dot( lightNormal, P - rectCoords[ 0 ] ) < 0.0 ) return vec3( 0.0 );
	vec3 T1, T2;
	T1 = normalize( V - N * dot( V, N ) );
	T2 = - cross( N, T1 );
	mat3 mat = mInv * transpose( mat3( T1, T2, N ) );
	vec3 coords[ 4 ];
	coords[ 0 ] = mat * ( rectCoords[ 0 ] - P );
	coords[ 1 ] = mat * ( rectCoords[ 1 ] - P );
	coords[ 2 ] = mat * ( rectCoords[ 2 ] - P );
	coords[ 3 ] = mat * ( rectCoords[ 3 ] - P );
	coords[ 0 ] = normalize( coords[ 0 ] );
	coords[ 1 ] = normalize( coords[ 1 ] );
	coords[ 2 ] = normalize( coords[ 2 ] );
	coords[ 3 ] = normalize( coords[ 3 ] );
	vec3 vectorFormFactor = vec3( 0.0 );
	vectorFormFactor += LTC_EdgeVectorFormFactor( coords[ 0 ], coords[ 1 ] );
	vectorFormFactor += LTC_EdgeVectorFormFactor( coords[ 1 ], coords[ 2 ] );
	vectorFormFactor += LTC_EdgeVectorFormFactor( coords[ 2 ], coords[ 3 ] );
	vectorFormFactor += LTC_EdgeVectorFormFactor( coords[ 3 ], coords[ 0 ] );
	float result = LTC_ClippedSphereFormFactor( vectorFormFactor );
	return vec3( result );
}
#if defined( USE_SHEEN )
float D_Charlie( float roughness, float dotNH ) {
	float alpha = pow2( roughness );
	float invAlpha = 1.0 / alpha;
	float cos2h = dotNH * dotNH;
	float sin2h = max( 1.0 - cos2h, 0.0078125 );
	return ( 2.0 + invAlpha ) * pow( sin2h, invAlpha * 0.5 ) / ( 2.0 * PI );
}
float V_Neubelt( float dotNV, float dotNL ) {
	return saturate( 1.0 / ( 4.0 * ( dotNL + dotNV - dotNL * dotNV ) ) );
}
vec3 BRDF_Sheen( const in vec3 lightDir, const in vec3 viewDir, const in vec3 normal, vec3 sheenColor, const in float sheenRoughness ) {
	vec3 halfDir = normalize( lightDir + viewDir );
	float dotNL = saturate( dot( normal, lightDir ) );
	float dotNV = saturate( dot( normal, viewDir ) );
	float dotNH = saturate( dot( normal, halfDir ) );
	float D = D_Charlie( sheenRoughness, dotNH );
	float V = V_Neubelt( dotNV, dotNL );
	return sheenColor * ( D * V );
}
#endif
float IBLSheenBRDF( const in vec3 normal, const in vec3 viewDir, const in float roughness ) {
	float dotNV = saturate( dot( normal, viewDir ) );
	float r2 = roughness * roughness;
	float rInv = 1.0 / ( roughness + 0.1 );
	float a = -1.9362 + 1.0678 * roughness + 0.4573 * r2 - 0.8469 * rInv;
	float b = -0.6014 + 0.5538 * roughness - 0.4670 * r2 - 0.1255 * rInv;
	float DG = exp( a * dotNV + b );
	return saturate( DG );
}
vec3 EnvironmentBRDF( const in vec3 normal, const in vec3 viewDir, const in vec3 specularColor, const in float specularF90, const in float roughness ) {
	float dotNV = saturate( dot( normal, viewDir ) );
	vec2 fab = texture2D( dfgLUT, vec2( roughness, dotNV ) ).rg;
	return specularColor * fab.x + specularF90 * fab.y;
}
#ifdef USE_IRIDESCENCE
void computeMultiscatteringIridescence( const in vec3 normal, const in vec3 viewDir, const in vec3 specularColor, const in float specularF90, const in float iridescence, const in vec3 iridescenceF0, const in float roughness, inout vec3 singleScatter, inout vec3 multiScatter ) {
#else
void computeMultiscattering( const in vec3 normal, const in vec3 viewDir, const in vec3 specularColor, const in float specularF90, const in float roughness, inout vec3 singleScatter, inout vec3 multiScatter ) {
#endif
	float dotNV = saturate( dot( normal, viewDir ) );
	vec2 fab = texture2D( dfgLUT, vec2( roughness, dotNV ) ).rg;
	#ifdef USE_IRIDESCENCE
		vec3 Fr = mix( specularColor, iridescenceF0, iridescence );
	#else
		vec3 Fr = specularColor;
	#endif
	vec3 FssEss = Fr * fab.x + specularF90 * fab.y;
	float Ess = fab.x + fab.y;
	float Ems = 1.0 - Ess;
	vec3 Favg = Fr + ( 1.0 - Fr ) * 0.047619;	vec3 Fms = FssEss * Favg / ( 1.0 - Ems * Favg );
	singleScatter += FssEss;
	multiScatter += Fms * Ems;
}
vec3 BRDF_GGX_Multiscatter( const in vec3 lightDir, const in vec3 viewDir, const in vec3 normal, const in PhysicalMaterial material ) {
	vec3 singleScatter = BRDF_GGX( lightDir, viewDir, normal, material );
	float dotNL = saturate( dot( normal, lightDir ) );
	float dotNV = saturate( dot( normal, viewDir ) );
	vec2 dfgV = texture2D( dfgLUT, vec2( material.roughness, dotNV ) ).rg;
	vec2 dfgL = texture2D( dfgLUT, vec2( material.roughness, dotNL ) ).rg;
	vec3 FssEss_V = material.specularColorBlended * dfgV.x + material.specularF90 * dfgV.y;
	vec3 FssEss_L = material.specularColorBlended * dfgL.x + material.specularF90 * dfgL.y;
	float Ess_V = dfgV.x + dfgV.y;
	float Ess_L = dfgL.x + dfgL.y;
	float Ems_V = 1.0 - Ess_V;
	float Ems_L = 1.0 - Ess_L;
	vec3 Favg = material.specularColorBlended + ( 1.0 - material.specularColorBlended ) * 0.047619;
	vec3 Fms = FssEss_V * FssEss_L * Favg / ( 1.0 - Ems_V * Ems_L * Favg + EPSILON );
	float compensationFactor = Ems_V * Ems_L;
	vec3 multiScatter = Fms * compensationFactor;
	return singleScatter + multiScatter;
}
#if NUM_RECT_AREA_LIGHTS > 0
	void RE_Direct_RectArea_Physical( const in RectAreaLight rectAreaLight, const in vec3 geometryPosition, const in vec3 geometryNormal, const in vec3 geometryViewDir, const in vec3 geometryClearcoatNormal, const in PhysicalMaterial material, inout ReflectedLight reflectedLight ) {
		vec3 normal = geometryNormal;
		vec3 viewDir = geometryViewDir;
		vec3 position = geometryPosition;
		vec3 lightPos = rectAreaLight.position;
		vec3 halfWidth = rectAreaLight.halfWidth;
		vec3 halfHeight = rectAreaLight.halfHeight;
		vec3 lightColor = rectAreaLight.color;
		float roughness = material.roughness;
		vec3 rectCoords[ 4 ];
		rectCoords[ 0 ] = lightPos + halfWidth - halfHeight;		rectCoords[ 1 ] = lightPos - halfWidth - halfHeight;
		rectCoords[ 2 ] = lightPos - halfWidth + halfHeight;
		rectCoords[ 3 ] = lightPos + halfWidth + halfHeight;
		vec2 uv = LTC_Uv( normal, viewDir, roughness );
		vec4 t1 = texture2D( ltc_1, uv );
		vec4 t2 = texture2D( ltc_2, uv );
		mat3 mInv = mat3(
			vec3( t1.x, 0, t1.y ),
			vec3(    0, 1,    0 ),
			vec3( t1.z, 0, t1.w )
		);
		vec3 fresnel = ( material.specularColorBlended * t2.x + ( material.specularF90 - material.specularColorBlended ) * t2.y );
		reflectedLight.directSpecular += lightColor * fresnel * LTC_Evaluate( normal, viewDir, position, mInv, rectCoords );
		reflectedLight.directDiffuse += lightColor * material.diffuseContribution * LTC_Evaluate( normal, viewDir, position, mat3( 1.0 ), rectCoords );
		#ifdef USE_CLEARCOAT
			vec3 Ncc = geometryClearcoatNormal;
			vec2 uvClearcoat = LTC_Uv( Ncc, viewDir, material.clearcoatRoughness );
			vec4 t1Clearcoat = texture2D( ltc_1, uvClearcoat );
			vec4 t2Clearcoat = texture2D( ltc_2, uvClearcoat );
			mat3 mInvClearcoat = mat3(
				vec3( t1Clearcoat.x, 0, t1Clearcoat.y ),
				vec3(             0, 1,             0 ),
				vec3( t1Clearcoat.z, 0, t1Clearcoat.w )
			);
			vec3 fresnelClearcoat = material.clearcoatF0 * t2Clearcoat.x + ( material.clearcoatF90 - material.clearcoatF0 ) * t2Clearcoat.y;
			clearcoatSpecularDirect += lightColor * fresnelClearcoat * LTC_Evaluate( Ncc, viewDir, position, mInvClearcoat, rectCoords );
		#endif
	}
#endif
void RE_Direct_Physical( const in IncidentLight directLight, const in vec3 geometryPosition, const in vec3 geometryNormal, const in vec3 geometryViewDir, const in vec3 geometryClearcoatNormal, const in PhysicalMaterial material, inout ReflectedLight reflectedLight ) {
	float dotNL = saturate( dot( geometryNormal, directLight.direction ) );
	vec3 irradiance = dotNL * directLight.color;
	#ifdef USE_CLEARCOAT
		float dotNLcc = saturate( dot( geometryClearcoatNormal, directLight.direction ) );
		vec3 ccIrradiance = dotNLcc * directLight.color;
		clearcoatSpecularDirect += ccIrradiance * BRDF_GGX_Clearcoat( directLight.direction, geometryViewDir, geometryClearcoatNormal, material );
	#endif
	#ifdef USE_SHEEN
 
 		sheenSpecularDirect += irradiance * BRDF_Sheen( directLight.direction, geometryViewDir, geometryNormal, material.sheenColor, material.sheenRoughness );
 
 		float sheenAlbedoV = IBLSheenBRDF( geometryNormal, geometryViewDir, material.sheenRoughness );
 		float sheenAlbedoL = IBLSheenBRDF( geometryNormal, directLight.direction, material.sheenRoughness );
 
 		float sheenEnergyComp = 1.0 - max3( material.sheenColor ) * max( sheenAlbedoV, sheenAlbedoL );
 
 		irradiance *= sheenEnergyComp;
 
 	#endif
	reflectedLight.directSpecular += irradiance * BRDF_GGX_Multiscatter( directLight.direction, geometryViewDir, geometryNormal, material );
	reflectedLight.directDiffuse += irradiance * BRDF_Lambert( material.diffuseContribution );
}
void RE_IndirectDiffuse_Physical( const in vec3 irradiance, const in vec3 geometryPosition, const in vec3 geometryNormal, const in vec3 geometryViewDir, const in vec3 geometryClearcoatNormal, const in PhysicalMaterial material, inout ReflectedLight reflectedLight ) {
	vec3 diffuse = irradiance * BRDF_Lambert( material.diffuseContribution );
	#ifdef USE_SHEEN
		float sheenAlbedo = IBLSheenBRDF( geometryNormal, geometryViewDir, material.sheenRoughness );
		float sheenEnergyComp = 1.0 - max3( material.sheenColor ) * sheenAlbedo;
		diffuse *= sheenEnergyComp;
	#endif
	reflectedLight.indirectDiffuse += diffuse;
}
void RE_IndirectSpecular_Physical( const in vec3 radiance, const in vec3 irradiance, const in vec3 clearcoatRadiance, const in vec3 geometryPosition, const in vec3 geometryNormal, const in vec3 geometryViewDir, const in vec3 geometryClearcoatNormal, const in PhysicalMaterial material, inout ReflectedLight reflectedLight) {
	#ifdef USE_CLEARCOAT
		clearcoatSpecularIndirect += clearcoatRadiance * EnvironmentBRDF( geometryClearcoatNormal, geometryViewDir, material.clearcoatF0, material.clearcoatF90, material.clearcoatRoughness );
	#endif
	#ifdef USE_SHEEN
		sheenSpecularIndirect += irradiance * material.sheenColor * IBLSheenBRDF( geometryNormal, geometryViewDir, material.sheenRoughness ) * RECIPROCAL_PI;
 	#endif
	vec3 singleScatteringDielectric = vec3( 0.0 );
	vec3 multiScatteringDielectric = vec3( 0.0 );
	vec3 singleScatteringMetallic = vec3( 0.0 );
	vec3 multiScatteringMetallic = vec3( 0.0 );
	#ifdef USE_IRIDESCENCE
		computeMultiscatteringIridescence( geometryNormal, geometryViewDir, material.specularColor, material.specularF90, material.iridescence, material.iridescenceFresnelDielectric, material.roughness, singleScatteringDielectric, multiScatteringDielectric );
		computeMultiscatteringIridescence( geometryNormal, geometryViewDir, material.diffuseColor, material.specularF90, material.iridescence, material.iridescenceFresnelMetallic, material.roughness, singleScatteringMetallic, multiScatteringMetallic );
	#else
		computeMultiscattering( geometryNormal, geometryViewDir, material.specularColor, material.specularF90, material.roughness, singleScatteringDielectric, multiScatteringDielectric );
		computeMultiscattering( geometryNormal, geometryViewDir, material.diffuseColor, material.specularF90, material.roughness, singleScatteringMetallic, multiScatteringMetallic );
	#endif
	vec3 singleScattering = mix( singleScatteringDielectric, singleScatteringMetallic, material.metalness );
	vec3 multiScattering = mix( multiScatteringDielectric, multiScatteringMetallic, material.metalness );
	vec3 totalScatteringDielectric = singleScatteringDielectric + multiScatteringDielectric;
	vec3 diffuse = material.diffuseContribution * ( 1.0 - totalScatteringDielectric );
	vec3 cosineWeightedIrradiance = irradiance * RECIPROCAL_PI;
	vec3 indirectSpecular = radiance * singleScattering;
	indirectSpecular += multiScattering * cosineWeightedIrradiance;
	vec3 indirectDiffuse = diffuse * cosineWeightedIrradiance;
	#ifdef USE_SHEEN
		float sheenAlbedo = IBLSheenBRDF( geometryNormal, geometryViewDir, material.sheenRoughness );
		float sheenEnergyComp = 1.0 - max3( material.sheenColor ) * sheenAlbedo;
		indirectSpecular *= sheenEnergyComp;
		indirectDiffuse *= sheenEnergyComp;
	#endif
	reflectedLight.indirectSpecular += indirectSpecular;
	reflectedLight.indirectDiffuse += indirectDiffuse;
}
#define RE_Direct				RE_Direct_Physical
#define RE_Direct_RectArea		RE_Direct_RectArea_Physical
#define RE_IndirectDiffuse		RE_IndirectDiffuse_Physical
#define RE_IndirectSpecular		RE_IndirectSpecular_Physical
float computeSpecularOcclusion( const in float dotNV, const in float ambientOcclusion, const in float roughness ) {
	return saturate( pow( dotNV + ambientOcclusion, exp2( - 16.0 * roughness - 1.0 ) ) - 1.0 + ambientOcclusion );
}`,GI=`
vec3 geometryPosition = - vViewPosition;
vec3 geometryNormal = normal;
vec3 geometryViewDir = ( isOrthographic ) ? vec3( 0, 0, 1 ) : normalize( vViewPosition );
vec3 geometryClearcoatNormal = vec3( 0.0 );
#ifdef USE_CLEARCOAT
	geometryClearcoatNormal = clearcoatNormal;
#endif
#ifdef USE_IRIDESCENCE
	float dotNVi = saturate( dot( normal, geometryViewDir ) );
	if ( material.iridescenceThickness == 0.0 ) {
		material.iridescence = 0.0;
	} else {
		material.iridescence = saturate( material.iridescence );
	}
	if ( material.iridescence > 0.0 ) {
		material.iridescenceFresnelDielectric = evalIridescence( 1.0, material.iridescenceIOR, dotNVi, material.iridescenceThickness, material.specularColor );
		material.iridescenceFresnelMetallic = evalIridescence( 1.0, material.iridescenceIOR, dotNVi, material.iridescenceThickness, material.diffuseColor );
		material.iridescenceFresnel = mix( material.iridescenceFresnelDielectric, material.iridescenceFresnelMetallic, material.metalness );
		material.iridescenceF0 = Schlick_to_F0( material.iridescenceFresnel, 1.0, dotNVi );
	}
#endif
IncidentLight directLight;
#if ( NUM_POINT_LIGHTS > 0 ) && defined( RE_Direct )
	PointLight pointLight;
	#if defined( USE_SHADOWMAP ) && NUM_POINT_LIGHT_SHADOWS > 0
	PointLightShadow pointLightShadow;
	#endif
	#pragma unroll_loop_start
	for ( int i = 0; i < NUM_POINT_LIGHTS; i ++ ) {
		pointLight = pointLights[ i ];
		getPointLightInfo( pointLight, geometryPosition, directLight );
		#if defined( USE_SHADOWMAP ) && ( UNROLLED_LOOP_INDEX < NUM_POINT_LIGHT_SHADOWS ) && ( defined( SHADOWMAP_TYPE_PCF ) || defined( SHADOWMAP_TYPE_BASIC ) )
		pointLightShadow = pointLightShadows[ i ];
		directLight.color *= ( directLight.visible && receiveShadow ) ? getPointShadow( pointShadowMap[ i ], pointLightShadow.shadowMapSize, pointLightShadow.shadowIntensity, pointLightShadow.shadowBias, pointLightShadow.shadowRadius, vPointShadowCoord[ i ], pointLightShadow.shadowCameraNear, pointLightShadow.shadowCameraFar ) : 1.0;
		#endif
		RE_Direct( directLight, geometryPosition, geometryNormal, geometryViewDir, geometryClearcoatNormal, material, reflectedLight );
	}
	#pragma unroll_loop_end
#endif
#if ( NUM_SPOT_LIGHTS > 0 ) && defined( RE_Direct )
	SpotLight spotLight;
	vec4 spotColor;
	vec3 spotLightCoord;
	bool inSpotLightMap;
	#if defined( USE_SHADOWMAP ) && NUM_SPOT_LIGHT_SHADOWS > 0
	SpotLightShadow spotLightShadow;
	#endif
	#pragma unroll_loop_start
	for ( int i = 0; i < NUM_SPOT_LIGHTS; i ++ ) {
		spotLight = spotLights[ i ];
		getSpotLightInfo( spotLight, geometryPosition, directLight );
		#if ( UNROLLED_LOOP_INDEX < NUM_SPOT_LIGHT_SHADOWS_WITH_MAPS )
		#define SPOT_LIGHT_MAP_INDEX UNROLLED_LOOP_INDEX
		#elif ( UNROLLED_LOOP_INDEX < NUM_SPOT_LIGHT_SHADOWS )
		#define SPOT_LIGHT_MAP_INDEX NUM_SPOT_LIGHT_MAPS
		#else
		#define SPOT_LIGHT_MAP_INDEX ( UNROLLED_LOOP_INDEX - NUM_SPOT_LIGHT_SHADOWS + NUM_SPOT_LIGHT_SHADOWS_WITH_MAPS )
		#endif
		#if ( SPOT_LIGHT_MAP_INDEX < NUM_SPOT_LIGHT_MAPS )
			spotLightCoord = vSpotLightCoord[ i ].xyz / vSpotLightCoord[ i ].w;
			inSpotLightMap = all( lessThan( abs( spotLightCoord * 2. - 1. ), vec3( 1.0 ) ) );
			spotColor = texture2D( spotLightMap[ SPOT_LIGHT_MAP_INDEX ], spotLightCoord.xy );
			directLight.color = inSpotLightMap ? directLight.color * spotColor.rgb : directLight.color;
		#endif
		#undef SPOT_LIGHT_MAP_INDEX
		#if defined( USE_SHADOWMAP ) && ( UNROLLED_LOOP_INDEX < NUM_SPOT_LIGHT_SHADOWS )
		spotLightShadow = spotLightShadows[ i ];
		directLight.color *= ( directLight.visible && receiveShadow ) ? getShadow( spotShadowMap[ i ], spotLightShadow.shadowMapSize, spotLightShadow.shadowIntensity, spotLightShadow.shadowBias, spotLightShadow.shadowRadius, vSpotLightCoord[ i ] ) : 1.0;
		#endif
		RE_Direct( directLight, geometryPosition, geometryNormal, geometryViewDir, geometryClearcoatNormal, material, reflectedLight );
	}
	#pragma unroll_loop_end
#endif
#if ( NUM_DIR_LIGHTS > 0 ) && defined( RE_Direct )
	DirectionalLight directionalLight;
	#if defined( USE_SHADOWMAP ) && NUM_DIR_LIGHT_SHADOWS > 0
	DirectionalLightShadow directionalLightShadow;
	#endif
	#pragma unroll_loop_start
	for ( int i = 0; i < NUM_DIR_LIGHTS; i ++ ) {
		directionalLight = directionalLights[ i ];
		getDirectionalLightInfo( directionalLight, directLight );
		#if defined( USE_SHADOWMAP ) && ( UNROLLED_LOOP_INDEX < NUM_DIR_LIGHT_SHADOWS )
		directionalLightShadow = directionalLightShadows[ i ];
		directLight.color *= ( directLight.visible && receiveShadow ) ? getShadow( directionalShadowMap[ i ], directionalLightShadow.shadowMapSize, directionalLightShadow.shadowIntensity, directionalLightShadow.shadowBias, directionalLightShadow.shadowRadius, vDirectionalShadowCoord[ i ] ) : 1.0;
		#endif
		RE_Direct( directLight, geometryPosition, geometryNormal, geometryViewDir, geometryClearcoatNormal, material, reflectedLight );
	}
	#pragma unroll_loop_end
#endif
#if ( NUM_RECT_AREA_LIGHTS > 0 ) && defined( RE_Direct_RectArea )
	RectAreaLight rectAreaLight;
	#pragma unroll_loop_start
	for ( int i = 0; i < NUM_RECT_AREA_LIGHTS; i ++ ) {
		rectAreaLight = rectAreaLights[ i ];
		RE_Direct_RectArea( rectAreaLight, geometryPosition, geometryNormal, geometryViewDir, geometryClearcoatNormal, material, reflectedLight );
	}
	#pragma unroll_loop_end
#endif
#if defined( RE_IndirectDiffuse )
	vec3 iblIrradiance = vec3( 0.0 );
	vec3 irradiance = getAmbientLightIrradiance( ambientLightColor );
	#if defined( USE_LIGHT_PROBES )
		irradiance += getLightProbeIrradiance( lightProbe, geometryNormal );
	#endif
	#if ( NUM_HEMI_LIGHTS > 0 )
		#pragma unroll_loop_start
		for ( int i = 0; i < NUM_HEMI_LIGHTS; i ++ ) {
			irradiance += getHemisphereLightIrradiance( hemisphereLights[ i ], geometryNormal );
		}
		#pragma unroll_loop_end
	#endif
#endif
#if defined( RE_IndirectSpecular )
	vec3 radiance = vec3( 0.0 );
	vec3 clearcoatRadiance = vec3( 0.0 );
#endif`,WI=`#if defined( RE_IndirectDiffuse )
	#ifdef USE_LIGHTMAP
		vec4 lightMapTexel = texture2D( lightMap, vLightMapUv );
		vec3 lightMapIrradiance = lightMapTexel.rgb * lightMapIntensity;
		irradiance += lightMapIrradiance;
	#endif
	#if defined( USE_ENVMAP ) && defined( ENVMAP_TYPE_CUBE_UV )
		#if defined( STANDARD ) || defined( LAMBERT ) || defined( PHONG )
			iblIrradiance += getIBLIrradiance( geometryNormal );
		#endif
	#endif
#endif
#if defined( USE_ENVMAP ) && defined( RE_IndirectSpecular )
	#ifdef USE_ANISOTROPY
		radiance += getIBLAnisotropyRadiance( geometryViewDir, geometryNormal, material.roughness, material.anisotropyB, material.anisotropy );
	#else
		radiance += getIBLRadiance( geometryViewDir, geometryNormal, material.roughness );
	#endif
	#ifdef USE_CLEARCOAT
		clearcoatRadiance += getIBLRadiance( geometryViewDir, geometryClearcoatNormal, material.clearcoatRoughness );
	#endif
#endif`,jI=`#if defined( RE_IndirectDiffuse )
	#if defined( LAMBERT ) || defined( PHONG )
		irradiance += iblIrradiance;
	#endif
	RE_IndirectDiffuse( irradiance, geometryPosition, geometryNormal, geometryViewDir, geometryClearcoatNormal, material, reflectedLight );
#endif
#if defined( RE_IndirectSpecular )
	RE_IndirectSpecular( radiance, iblIrradiance, clearcoatRadiance, geometryPosition, geometryNormal, geometryViewDir, geometryClearcoatNormal, material, reflectedLight );
#endif`,$I=`#if defined( USE_LOGARITHMIC_DEPTH_BUFFER )
	gl_FragDepth = vIsPerspective == 0.0 ? gl_FragCoord.z : log2( vFragDepth ) * logDepthBufFC * 0.5;
#endif`,qI=`#if defined( USE_LOGARITHMIC_DEPTH_BUFFER )
	uniform float logDepthBufFC;
	varying float vFragDepth;
	varying float vIsPerspective;
#endif`,XI=`#ifdef USE_LOGARITHMIC_DEPTH_BUFFER
	varying float vFragDepth;
	varying float vIsPerspective;
#endif`,YI=`#ifdef USE_LOGARITHMIC_DEPTH_BUFFER
	vFragDepth = 1.0 + gl_Position.w;
	vIsPerspective = float( isPerspectiveMatrix( projectionMatrix ) );
#endif`,ZI=`#ifdef USE_MAP
	vec4 sampledDiffuseColor = texture2D( map, vMapUv );
	#ifdef DECODE_VIDEO_TEXTURE
		sampledDiffuseColor = sRGBTransferEOTF( sampledDiffuseColor );
	#endif
	diffuseColor *= sampledDiffuseColor;
#endif`,JI=`#ifdef USE_MAP
	uniform sampler2D map;
#endif`,KI=`#if defined( USE_MAP ) || defined( USE_ALPHAMAP )
	#if defined( USE_POINTS_UV )
		vec2 uv = vUv;
	#else
		vec2 uv = ( uvTransform * vec3( gl_PointCoord.x, 1.0 - gl_PointCoord.y, 1 ) ).xy;
	#endif
#endif
#ifdef USE_MAP
	diffuseColor *= texture2D( map, uv );
#endif
#ifdef USE_ALPHAMAP
	diffuseColor.a *= texture2D( alphaMap, uv ).g;
#endif`,QI=`#if defined( USE_POINTS_UV )
	varying vec2 vUv;
#else
	#if defined( USE_MAP ) || defined( USE_ALPHAMAP )
		uniform mat3 uvTransform;
	#endif
#endif
#ifdef USE_MAP
	uniform sampler2D map;
#endif
#ifdef USE_ALPHAMAP
	uniform sampler2D alphaMap;
#endif`,eA=`float metalnessFactor = metalness;
#ifdef USE_METALNESSMAP
	vec4 texelMetalness = texture2D( metalnessMap, vMetalnessMapUv );
	metalnessFactor *= texelMetalness.b;
#endif`,tA=`#ifdef USE_METALNESSMAP
	uniform sampler2D metalnessMap;
#endif`,nA=`#ifdef USE_INSTANCING_MORPH
	float morphTargetInfluences[ MORPHTARGETS_COUNT ];
	float morphTargetBaseInfluence = texelFetch( morphTexture, ivec2( 0, gl_InstanceID ), 0 ).r;
	for ( int i = 0; i < MORPHTARGETS_COUNT; i ++ ) {
		morphTargetInfluences[i] =  texelFetch( morphTexture, ivec2( i + 1, gl_InstanceID ), 0 ).r;
	}
#endif`,iA=`#if defined( USE_MORPHCOLORS )
	vColor *= morphTargetBaseInfluence;
	for ( int i = 0; i < MORPHTARGETS_COUNT; i ++ ) {
		#if defined( USE_COLOR_ALPHA )
			if ( morphTargetInfluences[ i ] != 0.0 ) vColor += getMorph( gl_VertexID, i, 2 ) * morphTargetInfluences[ i ];
		#elif defined( USE_COLOR )
			if ( morphTargetInfluences[ i ] != 0.0 ) vColor += getMorph( gl_VertexID, i, 2 ).rgb * morphTargetInfluences[ i ];
		#endif
	}
#endif`,rA=`#ifdef USE_MORPHNORMALS
	objectNormal *= morphTargetBaseInfluence;
	for ( int i = 0; i < MORPHTARGETS_COUNT; i ++ ) {
		if ( morphTargetInfluences[ i ] != 0.0 ) objectNormal += getMorph( gl_VertexID, i, 1 ).xyz * morphTargetInfluences[ i ];
	}
#endif`,sA=`#ifdef USE_MORPHTARGETS
	#ifndef USE_INSTANCING_MORPH
		uniform float morphTargetBaseInfluence;
		uniform float morphTargetInfluences[ MORPHTARGETS_COUNT ];
	#endif
	uniform sampler2DArray morphTargetsTexture;
	uniform ivec2 morphTargetsTextureSize;
	vec4 getMorph( const in int vertexIndex, const in int morphTargetIndex, const in int offset ) {
		int texelIndex = vertexIndex * MORPHTARGETS_TEXTURE_STRIDE + offset;
		int y = texelIndex / morphTargetsTextureSize.x;
		int x = texelIndex - y * morphTargetsTextureSize.x;
		ivec3 morphUV = ivec3( x, y, morphTargetIndex );
		return texelFetch( morphTargetsTexture, morphUV, 0 );
	}
#endif`,oA=`#ifdef USE_MORPHTARGETS
	transformed *= morphTargetBaseInfluence;
	for ( int i = 0; i < MORPHTARGETS_COUNT; i ++ ) {
		if ( morphTargetInfluences[ i ] != 0.0 ) transformed += getMorph( gl_VertexID, i, 0 ).xyz * morphTargetInfluences[ i ];
	}
#endif`,aA=`float faceDirection = gl_FrontFacing ? 1.0 : - 1.0;
#ifdef FLAT_SHADED
	vec3 fdx = dFdx( vViewPosition );
	vec3 fdy = dFdy( vViewPosition );
	vec3 normal = normalize( cross( fdx, fdy ) );
#else
	vec3 normal = normalize( vNormal );
	#ifdef DOUBLE_SIDED
		normal *= faceDirection;
	#endif
#endif
#if defined( USE_NORMALMAP_TANGENTSPACE ) || defined( USE_CLEARCOAT_NORMALMAP ) || defined( USE_ANISOTROPY )
	#ifdef USE_TANGENT
		mat3 tbn = mat3( normalize( vTangent ), normalize( vBitangent ), normal );
	#else
		mat3 tbn = getTangentFrame( - vViewPosition, normal,
		#if defined( USE_NORMALMAP )
			vNormalMapUv
		#elif defined( USE_CLEARCOAT_NORMALMAP )
			vClearcoatNormalMapUv
		#else
			vUv
		#endif
		);
	#endif
	#if defined( DOUBLE_SIDED ) && ! defined( FLAT_SHADED )
		tbn[0] *= faceDirection;
		tbn[1] *= faceDirection;
	#endif
#endif
#ifdef USE_CLEARCOAT_NORMALMAP
	#ifdef USE_TANGENT
		mat3 tbn2 = mat3( normalize( vTangent ), normalize( vBitangent ), normal );
	#else
		mat3 tbn2 = getTangentFrame( - vViewPosition, normal, vClearcoatNormalMapUv );
	#endif
	#if defined( DOUBLE_SIDED ) && ! defined( FLAT_SHADED )
		tbn2[0] *= faceDirection;
		tbn2[1] *= faceDirection;
	#endif
#endif
vec3 nonPerturbedNormal = normal;`,cA=`#ifdef USE_NORMALMAP_OBJECTSPACE
	normal = texture2D( normalMap, vNormalMapUv ).xyz * 2.0 - 1.0;
	#ifdef FLIP_SIDED
		normal = - normal;
	#endif
	#ifdef DOUBLE_SIDED
		normal = normal * faceDirection;
	#endif
	normal = normalize( normalMatrix * normal );
#elif defined( USE_NORMALMAP_TANGENTSPACE )
	vec3 mapN = texture2D( normalMap, vNormalMapUv ).xyz * 2.0 - 1.0;
	mapN.xy *= normalScale;
	normal = normalize( tbn * mapN );
#elif defined( USE_BUMPMAP )
	normal = perturbNormalArb( - vViewPosition, normal, dHdxy_fwd(), faceDirection );
#endif`,lA=`#ifndef FLAT_SHADED
	varying vec3 vNormal;
	#ifdef USE_TANGENT
		varying vec3 vTangent;
		varying vec3 vBitangent;
	#endif
#endif`,uA=`#ifndef FLAT_SHADED
	varying vec3 vNormal;
	#ifdef USE_TANGENT
		varying vec3 vTangent;
		varying vec3 vBitangent;
	#endif
#endif`,dA=`#ifndef FLAT_SHADED
	vNormal = normalize( transformedNormal );
	#ifdef USE_TANGENT
		vTangent = normalize( transformedTangent );
		vBitangent = normalize( cross( vNormal, vTangent ) * tangent.w );
	#endif
#endif`,fA=`#ifdef USE_NORMALMAP
	uniform sampler2D normalMap;
	uniform vec2 normalScale;
#endif
#ifdef USE_NORMALMAP_OBJECTSPACE
	uniform mat3 normalMatrix;
#endif
#if ! defined ( USE_TANGENT ) && ( defined ( USE_NORMALMAP_TANGENTSPACE ) || defined ( USE_CLEARCOAT_NORMALMAP ) || defined( USE_ANISOTROPY ) )
	mat3 getTangentFrame( vec3 eye_pos, vec3 surf_norm, vec2 uv ) {
		vec3 q0 = dFdx( eye_pos.xyz );
		vec3 q1 = dFdy( eye_pos.xyz );
		vec2 st0 = dFdx( uv.st );
		vec2 st1 = dFdy( uv.st );
		vec3 N = surf_norm;
		vec3 q1perp = cross( q1, N );
		vec3 q0perp = cross( N, q0 );
		vec3 T = q1perp * st0.x + q0perp * st1.x;
		vec3 B = q1perp * st0.y + q0perp * st1.y;
		float det = max( dot( T, T ), dot( B, B ) );
		float scale = ( det == 0.0 ) ? 0.0 : inversesqrt( det );
		return mat3( T * scale, B * scale, N );
	}
#endif`,hA=`#ifdef USE_CLEARCOAT
	vec3 clearcoatNormal = nonPerturbedNormal;
#endif`,pA=`#ifdef USE_CLEARCOAT_NORMALMAP
	vec3 clearcoatMapN = texture2D( clearcoatNormalMap, vClearcoatNormalMapUv ).xyz * 2.0 - 1.0;
	clearcoatMapN.xy *= clearcoatNormalScale;
	clearcoatNormal = normalize( tbn2 * clearcoatMapN );
#endif`,mA=`#ifdef USE_CLEARCOATMAP
	uniform sampler2D clearcoatMap;
#endif
#ifdef USE_CLEARCOAT_NORMALMAP
	uniform sampler2D clearcoatNormalMap;
	uniform vec2 clearcoatNormalScale;
#endif
#ifdef USE_CLEARCOAT_ROUGHNESSMAP
	uniform sampler2D clearcoatRoughnessMap;
#endif`,gA=`#ifdef USE_IRIDESCENCEMAP
	uniform sampler2D iridescenceMap;
#endif
#ifdef USE_IRIDESCENCE_THICKNESSMAP
	uniform sampler2D iridescenceThicknessMap;
#endif`,vA=`#ifdef OPAQUE
diffuseColor.a = 1.0;
#endif
#ifdef USE_TRANSMISSION
diffuseColor.a *= material.transmissionAlpha;
#endif
gl_FragColor = vec4( outgoingLight, diffuseColor.a );`,yA=`vec3 packNormalToRGB( const in vec3 normal ) {
	return normalize( normal ) * 0.5 + 0.5;
}
vec3 unpackRGBToNormal( const in vec3 rgb ) {
	return 2.0 * rgb.xyz - 1.0;
}
const float PackUpscale = 256. / 255.;const float UnpackDownscale = 255. / 256.;const float ShiftRight8 = 1. / 256.;
const float Inv255 = 1. / 255.;
const vec4 PackFactors = vec4( 1.0, 256.0, 256.0 * 256.0, 256.0 * 256.0 * 256.0 );
const vec2 UnpackFactors2 = vec2( UnpackDownscale, 1.0 / PackFactors.g );
const vec3 UnpackFactors3 = vec3( UnpackDownscale / PackFactors.rg, 1.0 / PackFactors.b );
const vec4 UnpackFactors4 = vec4( UnpackDownscale / PackFactors.rgb, 1.0 / PackFactors.a );
vec4 packDepthToRGBA( const in float v ) {
	if( v <= 0.0 )
		return vec4( 0., 0., 0., 0. );
	if( v >= 1.0 )
		return vec4( 1., 1., 1., 1. );
	float vuf;
	float af = modf( v * PackFactors.a, vuf );
	float bf = modf( vuf * ShiftRight8, vuf );
	float gf = modf( vuf * ShiftRight8, vuf );
	return vec4( vuf * Inv255, gf * PackUpscale, bf * PackUpscale, af );
}
vec3 packDepthToRGB( const in float v ) {
	if( v <= 0.0 )
		return vec3( 0., 0., 0. );
	if( v >= 1.0 )
		return vec3( 1., 1., 1. );
	float vuf;
	float bf = modf( v * PackFactors.b, vuf );
	float gf = modf( vuf * ShiftRight8, vuf );
	return vec3( vuf * Inv255, gf * PackUpscale, bf );
}
vec2 packDepthToRG( const in float v ) {
	if( v <= 0.0 )
		return vec2( 0., 0. );
	if( v >= 1.0 )
		return vec2( 1., 1. );
	float vuf;
	float gf = modf( v * 256., vuf );
	return vec2( vuf * Inv255, gf );
}
float unpackRGBAToDepth( const in vec4 v ) {
	return dot( v, UnpackFactors4 );
}
float unpackRGBToDepth( const in vec3 v ) {
	return dot( v, UnpackFactors3 );
}
float unpackRGToDepth( const in vec2 v ) {
	return v.r * UnpackFactors2.r + v.g * UnpackFactors2.g;
}
vec4 pack2HalfToRGBA( const in vec2 v ) {
	vec4 r = vec4( v.x, fract( v.x * 255.0 ), v.y, fract( v.y * 255.0 ) );
	return vec4( r.x - r.y / 255.0, r.y, r.z - r.w / 255.0, r.w );
}
vec2 unpackRGBATo2Half( const in vec4 v ) {
	return vec2( v.x + ( v.y / 255.0 ), v.z + ( v.w / 255.0 ) );
}
float viewZToOrthographicDepth( const in float viewZ, const in float near, const in float far ) {
	return ( viewZ + near ) / ( near - far );
}
float orthographicDepthToViewZ( const in float depth, const in float near, const in float far ) {
	#ifdef USE_REVERSED_DEPTH_BUFFER
	
		return depth * ( far - near ) - far;
	#else
		return depth * ( near - far ) - near;
	#endif
}
float viewZToPerspectiveDepth( const in float viewZ, const in float near, const in float far ) {
	return ( ( near + viewZ ) * far ) / ( ( far - near ) * viewZ );
}
float perspectiveDepthToViewZ( const in float depth, const in float near, const in float far ) {
	
	#ifdef USE_REVERSED_DEPTH_BUFFER
		return ( near * far ) / ( ( near - far ) * depth - near );
	#else
		return ( near * far ) / ( ( far - near ) * depth - far );
	#endif
}`,_A=`#ifdef PREMULTIPLIED_ALPHA
	gl_FragColor.rgb *= gl_FragColor.a;
#endif`,xA=`vec4 mvPosition = vec4( transformed, 1.0 );
#ifdef USE_BATCHING
	mvPosition = batchingMatrix * mvPosition;
#endif
#ifdef USE_INSTANCING
	mvPosition = instanceMatrix * mvPosition;
#endif
mvPosition = modelViewMatrix * mvPosition;
gl_Position = projectionMatrix * mvPosition;`,MA=`#ifdef DITHERING
	gl_FragColor.rgb = dithering( gl_FragColor.rgb );
#endif`,bA=`#ifdef DITHERING
	vec3 dithering( vec3 color ) {
		float grid_position = rand( gl_FragCoord.xy );
		vec3 dither_shift_RGB = vec3( 0.25 / 255.0, -0.25 / 255.0, 0.25 / 255.0 );
		dither_shift_RGB = mix( 2.0 * dither_shift_RGB, -2.0 * dither_shift_RGB, grid_position );
		return color + dither_shift_RGB;
	}
#endif`,SA=`float roughnessFactor = roughness;
#ifdef USE_ROUGHNESSMAP
	vec4 texelRoughness = texture2D( roughnessMap, vRoughnessMapUv );
	roughnessFactor *= texelRoughness.g;
#endif`,EA=`#ifdef USE_ROUGHNESSMAP
	uniform sampler2D roughnessMap;
#endif`,TA=`#if NUM_SPOT_LIGHT_COORDS > 0
	varying vec4 vSpotLightCoord[ NUM_SPOT_LIGHT_COORDS ];
#endif
#if NUM_SPOT_LIGHT_MAPS > 0
	uniform sampler2D spotLightMap[ NUM_SPOT_LIGHT_MAPS ];
#endif
#ifdef USE_SHADOWMAP
	#if NUM_DIR_LIGHT_SHADOWS > 0
		#if defined( SHADOWMAP_TYPE_PCF )
			uniform sampler2DShadow directionalShadowMap[ NUM_DIR_LIGHT_SHADOWS ];
		#else
			uniform sampler2D directionalShadowMap[ NUM_DIR_LIGHT_SHADOWS ];
		#endif
		varying vec4 vDirectionalShadowCoord[ NUM_DIR_LIGHT_SHADOWS ];
		struct DirectionalLightShadow {
			float shadowIntensity;
			float shadowBias;
			float shadowNormalBias;
			float shadowRadius;
			vec2 shadowMapSize;
		};
		uniform DirectionalLightShadow directionalLightShadows[ NUM_DIR_LIGHT_SHADOWS ];
	#endif
	#if NUM_SPOT_LIGHT_SHADOWS > 0
		#if defined( SHADOWMAP_TYPE_PCF )
			uniform sampler2DShadow spotShadowMap[ NUM_SPOT_LIGHT_SHADOWS ];
		#else
			uniform sampler2D spotShadowMap[ NUM_SPOT_LIGHT_SHADOWS ];
		#endif
		struct SpotLightShadow {
			float shadowIntensity;
			float shadowBias;
			float shadowNormalBias;
			float shadowRadius;
			vec2 shadowMapSize;
		};
		uniform SpotLightShadow spotLightShadows[ NUM_SPOT_LIGHT_SHADOWS ];
	#endif
	#if NUM_POINT_LIGHT_SHADOWS > 0
		#if defined( SHADOWMAP_TYPE_PCF )
			uniform samplerCubeShadow pointShadowMap[ NUM_POINT_LIGHT_SHADOWS ];
		#elif defined( SHADOWMAP_TYPE_BASIC )
			uniform samplerCube pointShadowMap[ NUM_POINT_LIGHT_SHADOWS ];
		#endif
		varying vec4 vPointShadowCoord[ NUM_POINT_LIGHT_SHADOWS ];
		struct PointLightShadow {
			float shadowIntensity;
			float shadowBias;
			float shadowNormalBias;
			float shadowRadius;
			vec2 shadowMapSize;
			float shadowCameraNear;
			float shadowCameraFar;
		};
		uniform PointLightShadow pointLightShadows[ NUM_POINT_LIGHT_SHADOWS ];
	#endif
	#if defined( SHADOWMAP_TYPE_PCF )
		float interleavedGradientNoise( vec2 position ) {
			return fract( 52.9829189 * fract( dot( position, vec2( 0.06711056, 0.00583715 ) ) ) );
		}
		vec2 vogelDiskSample( int sampleIndex, int samplesCount, float phi ) {
			const float goldenAngle = 2.399963229728653;
			float r = sqrt( ( float( sampleIndex ) + 0.5 ) / float( samplesCount ) );
			float theta = float( sampleIndex ) * goldenAngle + phi;
			return vec2( cos( theta ), sin( theta ) ) * r;
		}
	#endif
	#if defined( SHADOWMAP_TYPE_PCF )
		float getShadow( sampler2DShadow shadowMap, vec2 shadowMapSize, float shadowIntensity, float shadowBias, float shadowRadius, vec4 shadowCoord ) {
			float shadow = 1.0;
			shadowCoord.xyz /= shadowCoord.w;
			shadowCoord.z += shadowBias;
			bool inFrustum = shadowCoord.x >= 0.0 && shadowCoord.x <= 1.0 && shadowCoord.y >= 0.0 && shadowCoord.y <= 1.0;
			bool frustumTest = inFrustum && shadowCoord.z <= 1.0;
			if ( frustumTest ) {
				vec2 texelSize = vec2( 1.0 ) / shadowMapSize;
				float radius = shadowRadius * texelSize.x;
				float phi = interleavedGradientNoise( gl_FragCoord.xy ) * PI2;
				shadow = (
					texture( shadowMap, vec3( shadowCoord.xy + vogelDiskSample( 0, 5, phi ) * radius, shadowCoord.z ) ) +
					texture( shadowMap, vec3( shadowCoord.xy + vogelDiskSample( 1, 5, phi ) * radius, shadowCoord.z ) ) +
					texture( shadowMap, vec3( shadowCoord.xy + vogelDiskSample( 2, 5, phi ) * radius, shadowCoord.z ) ) +
					texture( shadowMap, vec3( shadowCoord.xy + vogelDiskSample( 3, 5, phi ) * radius, shadowCoord.z ) ) +
					texture( shadowMap, vec3( shadowCoord.xy + vogelDiskSample( 4, 5, phi ) * radius, shadowCoord.z ) )
				) * 0.2;
			}
			return mix( 1.0, shadow, shadowIntensity );
		}
	#elif defined( SHADOWMAP_TYPE_VSM )
		float getShadow( sampler2D shadowMap, vec2 shadowMapSize, float shadowIntensity, float shadowBias, float shadowRadius, vec4 shadowCoord ) {
			float shadow = 1.0;
			shadowCoord.xyz /= shadowCoord.w;
			#ifdef USE_REVERSED_DEPTH_BUFFER
				shadowCoord.z -= shadowBias;
			#else
				shadowCoord.z += shadowBias;
			#endif
			bool inFrustum = shadowCoord.x >= 0.0 && shadowCoord.x <= 1.0 && shadowCoord.y >= 0.0 && shadowCoord.y <= 1.0;
			bool frustumTest = inFrustum && shadowCoord.z <= 1.0;
			if ( frustumTest ) {
				vec2 distribution = texture2D( shadowMap, shadowCoord.xy ).rg;
				float mean = distribution.x;
				float variance = distribution.y * distribution.y;
				#ifdef USE_REVERSED_DEPTH_BUFFER
					float hard_shadow = step( mean, shadowCoord.z );
				#else
					float hard_shadow = step( shadowCoord.z, mean );
				#endif
				
				if ( hard_shadow == 1.0 ) {
					shadow = 1.0;
				} else {
					variance = max( variance, 0.0000001 );
					float d = shadowCoord.z - mean;
					float p_max = variance / ( variance + d * d );
					p_max = clamp( ( p_max - 0.3 ) / 0.65, 0.0, 1.0 );
					shadow = max( hard_shadow, p_max );
				}
			}
			return mix( 1.0, shadow, shadowIntensity );
		}
	#else
		float getShadow( sampler2D shadowMap, vec2 shadowMapSize, float shadowIntensity, float shadowBias, float shadowRadius, vec4 shadowCoord ) {
			float shadow = 1.0;
			shadowCoord.xyz /= shadowCoord.w;
			#ifdef USE_REVERSED_DEPTH_BUFFER
				shadowCoord.z -= shadowBias;
			#else
				shadowCoord.z += shadowBias;
			#endif
			bool inFrustum = shadowCoord.x >= 0.0 && shadowCoord.x <= 1.0 && shadowCoord.y >= 0.0 && shadowCoord.y <= 1.0;
			bool frustumTest = inFrustum && shadowCoord.z <= 1.0;
			if ( frustumTest ) {
				float depth = texture2D( shadowMap, shadowCoord.xy ).r;
				#ifdef USE_REVERSED_DEPTH_BUFFER
					shadow = step( depth, shadowCoord.z );
				#else
					shadow = step( shadowCoord.z, depth );
				#endif
			}
			return mix( 1.0, shadow, shadowIntensity );
		}
	#endif
	#if NUM_POINT_LIGHT_SHADOWS > 0
	#if defined( SHADOWMAP_TYPE_PCF )
	float getPointShadow( samplerCubeShadow shadowMap, vec2 shadowMapSize, float shadowIntensity, float shadowBias, float shadowRadius, vec4 shadowCoord, float shadowCameraNear, float shadowCameraFar ) {
		float shadow = 1.0;
		vec3 lightToPosition = shadowCoord.xyz;
		vec3 bd3D = normalize( lightToPosition );
		vec3 absVec = abs( lightToPosition );
		float viewSpaceZ = max( max( absVec.x, absVec.y ), absVec.z );
		if ( viewSpaceZ - shadowCameraFar <= 0.0 && viewSpaceZ - shadowCameraNear >= 0.0 ) {
			#ifdef USE_REVERSED_DEPTH_BUFFER
				float dp = ( shadowCameraNear * ( shadowCameraFar - viewSpaceZ ) ) / ( viewSpaceZ * ( shadowCameraFar - shadowCameraNear ) );
				dp -= shadowBias;
			#else
				float dp = ( shadowCameraFar * ( viewSpaceZ - shadowCameraNear ) ) / ( viewSpaceZ * ( shadowCameraFar - shadowCameraNear ) );
				dp += shadowBias;
			#endif
			float texelSize = shadowRadius / shadowMapSize.x;
			vec3 absDir = abs( bd3D );
			vec3 tangent = absDir.x > absDir.z ? vec3( 0.0, 1.0, 0.0 ) : vec3( 1.0, 0.0, 0.0 );
			tangent = normalize( cross( bd3D, tangent ) );
			vec3 bitangent = cross( bd3D, tangent );
			float phi = interleavedGradientNoise( gl_FragCoord.xy ) * PI2;
			vec2 sample0 = vogelDiskSample( 0, 5, phi );
			vec2 sample1 = vogelDiskSample( 1, 5, phi );
			vec2 sample2 = vogelDiskSample( 2, 5, phi );
			vec2 sample3 = vogelDiskSample( 3, 5, phi );
			vec2 sample4 = vogelDiskSample( 4, 5, phi );
			shadow = (
				texture( shadowMap, vec4( bd3D + ( tangent * sample0.x + bitangent * sample0.y ) * texelSize, dp ) ) +
				texture( shadowMap, vec4( bd3D + ( tangent * sample1.x + bitangent * sample1.y ) * texelSize, dp ) ) +
				texture( shadowMap, vec4( bd3D + ( tangent * sample2.x + bitangent * sample2.y ) * texelSize, dp ) ) +
				texture( shadowMap, vec4( bd3D + ( tangent * sample3.x + bitangent * sample3.y ) * texelSize, dp ) ) +
				texture( shadowMap, vec4( bd3D + ( tangent * sample4.x + bitangent * sample4.y ) * texelSize, dp ) )
			) * 0.2;
		}
		return mix( 1.0, shadow, shadowIntensity );
	}
	#elif defined( SHADOWMAP_TYPE_BASIC )
	float getPointShadow( samplerCube shadowMap, vec2 shadowMapSize, float shadowIntensity, float shadowBias, float shadowRadius, vec4 shadowCoord, float shadowCameraNear, float shadowCameraFar ) {
		float shadow = 1.0;
		vec3 lightToPosition = shadowCoord.xyz;
		vec3 absVec = abs( lightToPosition );
		float viewSpaceZ = max( max( absVec.x, absVec.y ), absVec.z );
		if ( viewSpaceZ - shadowCameraFar <= 0.0 && viewSpaceZ - shadowCameraNear >= 0.0 ) {
			float dp = ( shadowCameraFar * ( viewSpaceZ - shadowCameraNear ) ) / ( viewSpaceZ * ( shadowCameraFar - shadowCameraNear ) );
			dp += shadowBias;
			vec3 bd3D = normalize( lightToPosition );
			float depth = textureCube( shadowMap, bd3D ).r;
			#ifdef USE_REVERSED_DEPTH_BUFFER
				depth = 1.0 - depth;
			#endif
			shadow = step( dp, depth );
		}
		return mix( 1.0, shadow, shadowIntensity );
	}
	#endif
	#endif
#endif`,wA=`#if NUM_SPOT_LIGHT_COORDS > 0
	uniform mat4 spotLightMatrix[ NUM_SPOT_LIGHT_COORDS ];
	varying vec4 vSpotLightCoord[ NUM_SPOT_LIGHT_COORDS ];
#endif
#ifdef USE_SHADOWMAP
	#if NUM_DIR_LIGHT_SHADOWS > 0
		uniform mat4 directionalShadowMatrix[ NUM_DIR_LIGHT_SHADOWS ];
		varying vec4 vDirectionalShadowCoord[ NUM_DIR_LIGHT_SHADOWS ];
		struct DirectionalLightShadow {
			float shadowIntensity;
			float shadowBias;
			float shadowNormalBias;
			float shadowRadius;
			vec2 shadowMapSize;
		};
		uniform DirectionalLightShadow directionalLightShadows[ NUM_DIR_LIGHT_SHADOWS ];
	#endif
	#if NUM_SPOT_LIGHT_SHADOWS > 0
		struct SpotLightShadow {
			float shadowIntensity;
			float shadowBias;
			float shadowNormalBias;
			float shadowRadius;
			vec2 shadowMapSize;
		};
		uniform SpotLightShadow spotLightShadows[ NUM_SPOT_LIGHT_SHADOWS ];
	#endif
	#if NUM_POINT_LIGHT_SHADOWS > 0
		uniform mat4 pointShadowMatrix[ NUM_POINT_LIGHT_SHADOWS ];
		varying vec4 vPointShadowCoord[ NUM_POINT_LIGHT_SHADOWS ];
		struct PointLightShadow {
			float shadowIntensity;
			float shadowBias;
			float shadowNormalBias;
			float shadowRadius;
			vec2 shadowMapSize;
			float shadowCameraNear;
			float shadowCameraFar;
		};
		uniform PointLightShadow pointLightShadows[ NUM_POINT_LIGHT_SHADOWS ];
	#endif
#endif`,CA=`#if ( defined( USE_SHADOWMAP ) && ( NUM_DIR_LIGHT_SHADOWS > 0 || NUM_POINT_LIGHT_SHADOWS > 0 ) ) || ( NUM_SPOT_LIGHT_COORDS > 0 )
	vec3 shadowWorldNormal = inverseTransformDirection( transformedNormal, viewMatrix );
	vec4 shadowWorldPosition;
#endif
#if defined( USE_SHADOWMAP )
	#if NUM_DIR_LIGHT_SHADOWS > 0
		#pragma unroll_loop_start
		for ( int i = 0; i < NUM_DIR_LIGHT_SHADOWS; i ++ ) {
			shadowWorldPosition = worldPosition + vec4( shadowWorldNormal * directionalLightShadows[ i ].shadowNormalBias, 0 );
			vDirectionalShadowCoord[ i ] = directionalShadowMatrix[ i ] * shadowWorldPosition;
		}
		#pragma unroll_loop_end
	#endif
	#if NUM_POINT_LIGHT_SHADOWS > 0
		#pragma unroll_loop_start
		for ( int i = 0; i < NUM_POINT_LIGHT_SHADOWS; i ++ ) {
			shadowWorldPosition = worldPosition + vec4( shadowWorldNormal * pointLightShadows[ i ].shadowNormalBias, 0 );
			vPointShadowCoord[ i ] = pointShadowMatrix[ i ] * shadowWorldPosition;
		}
		#pragma unroll_loop_end
	#endif
#endif
#if NUM_SPOT_LIGHT_COORDS > 0
	#pragma unroll_loop_start
	for ( int i = 0; i < NUM_SPOT_LIGHT_COORDS; i ++ ) {
		shadowWorldPosition = worldPosition;
		#if ( defined( USE_SHADOWMAP ) && UNROLLED_LOOP_INDEX < NUM_SPOT_LIGHT_SHADOWS )
			shadowWorldPosition.xyz += shadowWorldNormal * spotLightShadows[ i ].shadowNormalBias;
		#endif
		vSpotLightCoord[ i ] = spotLightMatrix[ i ] * shadowWorldPosition;
	}
	#pragma unroll_loop_end
#endif`,IA=`float getShadowMask() {
	float shadow = 1.0;
	#ifdef USE_SHADOWMAP
	#if NUM_DIR_LIGHT_SHADOWS > 0
	DirectionalLightShadow directionalLight;
	#pragma unroll_loop_start
	for ( int i = 0; i < NUM_DIR_LIGHT_SHADOWS; i ++ ) {
		directionalLight = directionalLightShadows[ i ];
		shadow *= receiveShadow ? getShadow( directionalShadowMap[ i ], directionalLight.shadowMapSize, directionalLight.shadowIntensity, directionalLight.shadowBias, directionalLight.shadowRadius, vDirectionalShadowCoord[ i ] ) : 1.0;
	}
	#pragma unroll_loop_end
	#endif
	#if NUM_SPOT_LIGHT_SHADOWS > 0
	SpotLightShadow spotLight;
	#pragma unroll_loop_start
	for ( int i = 0; i < NUM_SPOT_LIGHT_SHADOWS; i ++ ) {
		spotLight = spotLightShadows[ i ];
		shadow *= receiveShadow ? getShadow( spotShadowMap[ i ], spotLight.shadowMapSize, spotLight.shadowIntensity, spotLight.shadowBias, spotLight.shadowRadius, vSpotLightCoord[ i ] ) : 1.0;
	}
	#pragma unroll_loop_end
	#endif
	#if NUM_POINT_LIGHT_SHADOWS > 0 && ( defined( SHADOWMAP_TYPE_PCF ) || defined( SHADOWMAP_TYPE_BASIC ) )
	PointLightShadow pointLight;
	#pragma unroll_loop_start
	for ( int i = 0; i < NUM_POINT_LIGHT_SHADOWS; i ++ ) {
		pointLight = pointLightShadows[ i ];
		shadow *= receiveShadow ? getPointShadow( pointShadowMap[ i ], pointLight.shadowMapSize, pointLight.shadowIntensity, pointLight.shadowBias, pointLight.shadowRadius, vPointShadowCoord[ i ], pointLight.shadowCameraNear, pointLight.shadowCameraFar ) : 1.0;
	}
	#pragma unroll_loop_end
	#endif
	#endif
	return shadow;
}`,AA=`#ifdef USE_SKINNING
	mat4 boneMatX = getBoneMatrix( skinIndex.x );
	mat4 boneMatY = getBoneMatrix( skinIndex.y );
	mat4 boneMatZ = getBoneMatrix( skinIndex.z );
	mat4 boneMatW = getBoneMatrix( skinIndex.w );
#endif`,DA=`#ifdef USE_SKINNING
	uniform mat4 bindMatrix;
	uniform mat4 bindMatrixInverse;
	uniform highp sampler2D boneTexture;
	mat4 getBoneMatrix( const in float i ) {
		int size = textureSize( boneTexture, 0 ).x;
		int j = int( i ) * 4;
		int x = j % size;
		int y = j / size;
		vec4 v1 = texelFetch( boneTexture, ivec2( x, y ), 0 );
		vec4 v2 = texelFetch( boneTexture, ivec2( x + 1, y ), 0 );
		vec4 v3 = texelFetch( boneTexture, ivec2( x + 2, y ), 0 );
		vec4 v4 = texelFetch( boneTexture, ivec2( x + 3, y ), 0 );
		return mat4( v1, v2, v3, v4 );
	}
#endif`,RA=`#ifdef USE_SKINNING
	vec4 skinVertex = bindMatrix * vec4( transformed, 1.0 );
	vec4 skinned = vec4( 0.0 );
	skinned += boneMatX * skinVertex * skinWeight.x;
	skinned += boneMatY * skinVertex * skinWeight.y;
	skinned += boneMatZ * skinVertex * skinWeight.z;
	skinned += boneMatW * skinVertex * skinWeight.w;
	transformed = ( bindMatrixInverse * skinned ).xyz;
#endif`,NA=`#ifdef USE_SKINNING
	mat4 skinMatrix = mat4( 0.0 );
	skinMatrix += skinWeight.x * boneMatX;
	skinMatrix += skinWeight.y * boneMatY;
	skinMatrix += skinWeight.z * boneMatZ;
	skinMatrix += skinWeight.w * boneMatW;
	skinMatrix = bindMatrixInverse * skinMatrix * bindMatrix;
	objectNormal = vec4( skinMatrix * vec4( objectNormal, 0.0 ) ).xyz;
	#ifdef USE_TANGENT
		objectTangent = vec4( skinMatrix * vec4( objectTangent, 0.0 ) ).xyz;
	#endif
#endif`,PA=`float specularStrength;
#ifdef USE_SPECULARMAP
	vec4 texelSpecular = texture2D( specularMap, vSpecularMapUv );
	specularStrength = texelSpecular.r;
#else
	specularStrength = 1.0;
#endif`,LA=`#ifdef USE_SPECULARMAP
	uniform sampler2D specularMap;
#endif`,OA=`#if defined( TONE_MAPPING )
	gl_FragColor.rgb = toneMapping( gl_FragColor.rgb );
#endif`,FA=`#ifndef saturate
#define saturate( a ) clamp( a, 0.0, 1.0 )
#endif
uniform float toneMappingExposure;
vec3 LinearToneMapping( vec3 color ) {
	return saturate( toneMappingExposure * color );
}
vec3 ReinhardToneMapping( vec3 color ) {
	color *= toneMappingExposure;
	return saturate( color / ( vec3( 1.0 ) + color ) );
}
vec3 CineonToneMapping( vec3 color ) {
	color *= toneMappingExposure;
	color = max( vec3( 0.0 ), color - 0.004 );
	return pow( ( color * ( 6.2 * color + 0.5 ) ) / ( color * ( 6.2 * color + 1.7 ) + 0.06 ), vec3( 2.2 ) );
}
vec3 RRTAndODTFit( vec3 v ) {
	vec3 a = v * ( v + 0.0245786 ) - 0.000090537;
	vec3 b = v * ( 0.983729 * v + 0.4329510 ) + 0.238081;
	return a / b;
}
vec3 ACESFilmicToneMapping( vec3 color ) {
	const mat3 ACESInputMat = mat3(
		vec3( 0.59719, 0.07600, 0.02840 ),		vec3( 0.35458, 0.90834, 0.13383 ),
		vec3( 0.04823, 0.01566, 0.83777 )
	);
	const mat3 ACESOutputMat = mat3(
		vec3(  1.60475, -0.10208, -0.00327 ),		vec3( -0.53108,  1.10813, -0.07276 ),
		vec3( -0.07367, -0.00605,  1.07602 )
	);
	color *= toneMappingExposure / 0.6;
	color = ACESInputMat * color;
	color = RRTAndODTFit( color );
	color = ACESOutputMat * color;
	return saturate( color );
}
const mat3 LINEAR_REC2020_TO_LINEAR_SRGB = mat3(
	vec3( 1.6605, - 0.1246, - 0.0182 ),
	vec3( - 0.5876, 1.1329, - 0.1006 ),
	vec3( - 0.0728, - 0.0083, 1.1187 )
);
const mat3 LINEAR_SRGB_TO_LINEAR_REC2020 = mat3(
	vec3( 0.6274, 0.0691, 0.0164 ),
	vec3( 0.3293, 0.9195, 0.0880 ),
	vec3( 0.0433, 0.0113, 0.8956 )
);
vec3 agxDefaultContrastApprox( vec3 x ) {
	vec3 x2 = x * x;
	vec3 x4 = x2 * x2;
	return + 15.5 * x4 * x2
		- 40.14 * x4 * x
		+ 31.96 * x4
		- 6.868 * x2 * x
		+ 0.4298 * x2
		+ 0.1191 * x
		- 0.00232;
}
vec3 AgXToneMapping( vec3 color ) {
	const mat3 AgXInsetMatrix = mat3(
		vec3( 0.856627153315983, 0.137318972929847, 0.11189821299995 ),
		vec3( 0.0951212405381588, 0.761241990602591, 0.0767994186031903 ),
		vec3( 0.0482516061458583, 0.101439036467562, 0.811302368396859 )
	);
	const mat3 AgXOutsetMatrix = mat3(
		vec3( 1.1271005818144368, - 0.1413297634984383, - 0.14132976349843826 ),
		vec3( - 0.11060664309660323, 1.157823702216272, - 0.11060664309660294 ),
		vec3( - 0.016493938717834573, - 0.016493938717834257, 1.2519364065950405 )
	);
	const float AgxMinEv = - 12.47393;	const float AgxMaxEv = 4.026069;
	color *= toneMappingExposure;
	color = LINEAR_SRGB_TO_LINEAR_REC2020 * color;
	color = AgXInsetMatrix * color;
	color = max( color, 1e-10 );	color = log2( color );
	color = ( color - AgxMinEv ) / ( AgxMaxEv - AgxMinEv );
	color = clamp( color, 0.0, 1.0 );
	color = agxDefaultContrastApprox( color );
	color = AgXOutsetMatrix * color;
	color = pow( max( vec3( 0.0 ), color ), vec3( 2.2 ) );
	color = LINEAR_REC2020_TO_LINEAR_SRGB * color;
	color = clamp( color, 0.0, 1.0 );
	return color;
}
vec3 NeutralToneMapping( vec3 color ) {
	const float StartCompression = 0.8 - 0.04;
	const float Desaturation = 0.15;
	color *= toneMappingExposure;
	float x = min( color.r, min( color.g, color.b ) );
	float offset = x < 0.08 ? x - 6.25 * x * x : 0.04;
	color -= offset;
	float peak = max( color.r, max( color.g, color.b ) );
	if ( peak < StartCompression ) return color;
	float d = 1. - StartCompression;
	float newPeak = 1. - d * d / ( peak + d - StartCompression );
	color *= newPeak / peak;
	float g = 1. - 1. / ( Desaturation * ( peak - newPeak ) + 1. );
	return mix( color, vec3( newPeak ), g );
}
vec3 CustomToneMapping( vec3 color ) { return color; }`,kA=`#ifdef USE_TRANSMISSION
	material.transmission = transmission;
	material.transmissionAlpha = 1.0;
	material.thickness = thickness;
	material.attenuationDistance = attenuationDistance;
	material.attenuationColor = attenuationColor;
	#ifdef USE_TRANSMISSIONMAP
		material.transmission *= texture2D( transmissionMap, vTransmissionMapUv ).r;
	#endif
	#ifdef USE_THICKNESSMAP
		material.thickness *= texture2D( thicknessMap, vThicknessMapUv ).g;
	#endif
	vec3 pos = vWorldPosition;
	vec3 v = normalize( cameraPosition - pos );
	vec3 n = inverseTransformDirection( normal, viewMatrix );
	vec4 transmitted = getIBLVolumeRefraction(
		n, v, material.roughness, material.diffuseContribution, material.specularColorBlended, material.specularF90,
		pos, modelMatrix, viewMatrix, projectionMatrix, material.dispersion, material.ior, material.thickness,
		material.attenuationColor, material.attenuationDistance );
	material.transmissionAlpha = mix( material.transmissionAlpha, transmitted.a, material.transmission );
	totalDiffuse = mix( totalDiffuse, transmitted.rgb, material.transmission );
#endif`,UA=`#ifdef USE_TRANSMISSION
	uniform float transmission;
	uniform float thickness;
	uniform float attenuationDistance;
	uniform vec3 attenuationColor;
	#ifdef USE_TRANSMISSIONMAP
		uniform sampler2D transmissionMap;
	#endif
	#ifdef USE_THICKNESSMAP
		uniform sampler2D thicknessMap;
	#endif
	uniform vec2 transmissionSamplerSize;
	uniform sampler2D transmissionSamplerMap;
	uniform mat4 modelMatrix;
	uniform mat4 projectionMatrix;
	varying vec3 vWorldPosition;
	float w0( float a ) {
		return ( 1.0 / 6.0 ) * ( a * ( a * ( - a + 3.0 ) - 3.0 ) + 1.0 );
	}
	float w1( float a ) {
		return ( 1.0 / 6.0 ) * ( a *  a * ( 3.0 * a - 6.0 ) + 4.0 );
	}
	float w2( float a ){
		return ( 1.0 / 6.0 ) * ( a * ( a * ( - 3.0 * a + 3.0 ) + 3.0 ) + 1.0 );
	}
	float w3( float a ) {
		return ( 1.0 / 6.0 ) * ( a * a * a );
	}
	float g0( float a ) {
		return w0( a ) + w1( a );
	}
	float g1( float a ) {
		return w2( a ) + w3( a );
	}
	float h0( float a ) {
		return - 1.0 + w1( a ) / ( w0( a ) + w1( a ) );
	}
	float h1( float a ) {
		return 1.0 + w3( a ) / ( w2( a ) + w3( a ) );
	}
	vec4 bicubic( sampler2D tex, vec2 uv, vec4 texelSize, float lod ) {
		uv = uv * texelSize.zw + 0.5;
		vec2 iuv = floor( uv );
		vec2 fuv = fract( uv );
		float g0x = g0( fuv.x );
		float g1x = g1( fuv.x );
		float h0x = h0( fuv.x );
		float h1x = h1( fuv.x );
		float h0y = h0( fuv.y );
		float h1y = h1( fuv.y );
		vec2 p0 = ( vec2( iuv.x + h0x, iuv.y + h0y ) - 0.5 ) * texelSize.xy;
		vec2 p1 = ( vec2( iuv.x + h1x, iuv.y + h0y ) - 0.5 ) * texelSize.xy;
		vec2 p2 = ( vec2( iuv.x + h0x, iuv.y + h1y ) - 0.5 ) * texelSize.xy;
		vec2 p3 = ( vec2( iuv.x + h1x, iuv.y + h1y ) - 0.5 ) * texelSize.xy;
		return g0( fuv.y ) * ( g0x * textureLod( tex, p0, lod ) + g1x * textureLod( tex, p1, lod ) ) +
			g1( fuv.y ) * ( g0x * textureLod( tex, p2, lod ) + g1x * textureLod( tex, p3, lod ) );
	}
	vec4 textureBicubic( sampler2D sampler, vec2 uv, float lod ) {
		vec2 fLodSize = vec2( textureSize( sampler, int( lod ) ) );
		vec2 cLodSize = vec2( textureSize( sampler, int( lod + 1.0 ) ) );
		vec2 fLodSizeInv = 1.0 / fLodSize;
		vec2 cLodSizeInv = 1.0 / cLodSize;
		vec4 fSample = bicubic( sampler, uv, vec4( fLodSizeInv, fLodSize ), floor( lod ) );
		vec4 cSample = bicubic( sampler, uv, vec4( cLodSizeInv, cLodSize ), ceil( lod ) );
		return mix( fSample, cSample, fract( lod ) );
	}
	vec3 getVolumeTransmissionRay( const in vec3 n, const in vec3 v, const in float thickness, const in float ior, const in mat4 modelMatrix ) {
		vec3 refractionVector = refract( - v, normalize( n ), 1.0 / ior );
		vec3 modelScale;
		modelScale.x = length( vec3( modelMatrix[ 0 ].xyz ) );
		modelScale.y = length( vec3( modelMatrix[ 1 ].xyz ) );
		modelScale.z = length( vec3( modelMatrix[ 2 ].xyz ) );
		return normalize( refractionVector ) * thickness * modelScale;
	}
	float applyIorToRoughness( const in float roughness, const in float ior ) {
		return roughness * clamp( ior * 2.0 - 2.0, 0.0, 1.0 );
	}
	vec4 getTransmissionSample( const in vec2 fragCoord, const in float roughness, const in float ior ) {
		float lod = log2( transmissionSamplerSize.x ) * applyIorToRoughness( roughness, ior );
		return textureBicubic( transmissionSamplerMap, fragCoord.xy, lod );
	}
	vec3 volumeAttenuation( const in float transmissionDistance, const in vec3 attenuationColor, const in float attenuationDistance ) {
		if ( isinf( attenuationDistance ) ) {
			return vec3( 1.0 );
		} else {
			vec3 attenuationCoefficient = -log( attenuationColor ) / attenuationDistance;
			vec3 transmittance = exp( - attenuationCoefficient * transmissionDistance );			return transmittance;
		}
	}
	vec4 getIBLVolumeRefraction( const in vec3 n, const in vec3 v, const in float roughness, const in vec3 diffuseColor,
		const in vec3 specularColor, const in float specularF90, const in vec3 position, const in mat4 modelMatrix,
		const in mat4 viewMatrix, const in mat4 projMatrix, const in float dispersion, const in float ior, const in float thickness,
		const in vec3 attenuationColor, const in float attenuationDistance ) {
		vec4 transmittedLight;
		vec3 transmittance;
		#ifdef USE_DISPERSION
			float halfSpread = ( ior - 1.0 ) * 0.025 * dispersion;
			vec3 iors = vec3( ior - halfSpread, ior, ior + halfSpread );
			for ( int i = 0; i < 3; i ++ ) {
				vec3 transmissionRay = getVolumeTransmissionRay( n, v, thickness, iors[ i ], modelMatrix );
				vec3 refractedRayExit = position + transmissionRay;
				vec4 ndcPos = projMatrix * viewMatrix * vec4( refractedRayExit, 1.0 );
				vec2 refractionCoords = ndcPos.xy / ndcPos.w;
				refractionCoords += 1.0;
				refractionCoords /= 2.0;
				vec4 transmissionSample = getTransmissionSample( refractionCoords, roughness, iors[ i ] );
				transmittedLight[ i ] = transmissionSample[ i ];
				transmittedLight.a += transmissionSample.a;
				transmittance[ i ] = diffuseColor[ i ] * volumeAttenuation( length( transmissionRay ), attenuationColor, attenuationDistance )[ i ];
			}
			transmittedLight.a /= 3.0;
		#else
			vec3 transmissionRay = getVolumeTransmissionRay( n, v, thickness, ior, modelMatrix );
			vec3 refractedRayExit = position + transmissionRay;
			vec4 ndcPos = projMatrix * viewMatrix * vec4( refractedRayExit, 1.0 );
			vec2 refractionCoords = ndcPos.xy / ndcPos.w;
			refractionCoords += 1.0;
			refractionCoords /= 2.0;
			transmittedLight = getTransmissionSample( refractionCoords, roughness, ior );
			transmittance = diffuseColor * volumeAttenuation( length( transmissionRay ), attenuationColor, attenuationDistance );
		#endif
		vec3 attenuatedColor = transmittance * transmittedLight.rgb;
		vec3 F = EnvironmentBRDF( n, v, specularColor, specularF90, roughness );
		float transmittanceFactor = ( transmittance.r + transmittance.g + transmittance.b ) / 3.0;
		return vec4( ( 1.0 - F ) * attenuatedColor, 1.0 - ( 1.0 - transmittedLight.a ) * transmittanceFactor );
	}
#endif`,BA=`#if defined( USE_UV ) || defined( USE_ANISOTROPY )
	varying vec2 vUv;
#endif
#ifdef USE_MAP
	varying vec2 vMapUv;
#endif
#ifdef USE_ALPHAMAP
	varying vec2 vAlphaMapUv;
#endif
#ifdef USE_LIGHTMAP
	varying vec2 vLightMapUv;
#endif
#ifdef USE_AOMAP
	varying vec2 vAoMapUv;
#endif
#ifdef USE_BUMPMAP
	varying vec2 vBumpMapUv;
#endif
#ifdef USE_NORMALMAP
	varying vec2 vNormalMapUv;
#endif
#ifdef USE_EMISSIVEMAP
	varying vec2 vEmissiveMapUv;
#endif
#ifdef USE_METALNESSMAP
	varying vec2 vMetalnessMapUv;
#endif
#ifdef USE_ROUGHNESSMAP
	varying vec2 vRoughnessMapUv;
#endif
#ifdef USE_ANISOTROPYMAP
	varying vec2 vAnisotropyMapUv;
#endif
#ifdef USE_CLEARCOATMAP
	varying vec2 vClearcoatMapUv;
#endif
#ifdef USE_CLEARCOAT_NORMALMAP
	varying vec2 vClearcoatNormalMapUv;
#endif
#ifdef USE_CLEARCOAT_ROUGHNESSMAP
	varying vec2 vClearcoatRoughnessMapUv;
#endif
#ifdef USE_IRIDESCENCEMAP
	varying vec2 vIridescenceMapUv;
#endif
#ifdef USE_IRIDESCENCE_THICKNESSMAP
	varying vec2 vIridescenceThicknessMapUv;
#endif
#ifdef USE_SHEEN_COLORMAP
	varying vec2 vSheenColorMapUv;
#endif
#ifdef USE_SHEEN_ROUGHNESSMAP
	varying vec2 vSheenRoughnessMapUv;
#endif
#ifdef USE_SPECULARMAP
	varying vec2 vSpecularMapUv;
#endif
#ifdef USE_SPECULAR_COLORMAP
	varying vec2 vSpecularColorMapUv;
#endif
#ifdef USE_SPECULAR_INTENSITYMAP
	varying vec2 vSpecularIntensityMapUv;
#endif
#ifdef USE_TRANSMISSIONMAP
	uniform mat3 transmissionMapTransform;
	varying vec2 vTransmissionMapUv;
#endif
#ifdef USE_THICKNESSMAP
	uniform mat3 thicknessMapTransform;
	varying vec2 vThicknessMapUv;
#endif`,VA=`#if defined( USE_UV ) || defined( USE_ANISOTROPY )
	varying vec2 vUv;
#endif
#ifdef USE_MAP
	uniform mat3 mapTransform;
	varying vec2 vMapUv;
#endif
#ifdef USE_ALPHAMAP
	uniform mat3 alphaMapTransform;
	varying vec2 vAlphaMapUv;
#endif
#ifdef USE_LIGHTMAP
	uniform mat3 lightMapTransform;
	varying vec2 vLightMapUv;
#endif
#ifdef USE_AOMAP
	uniform mat3 aoMapTransform;
	varying vec2 vAoMapUv;
#endif
#ifdef USE_BUMPMAP
	uniform mat3 bumpMapTransform;
	varying vec2 vBumpMapUv;
#endif
#ifdef USE_NORMALMAP
	uniform mat3 normalMapTransform;
	varying vec2 vNormalMapUv;
#endif
#ifdef USE_DISPLACEMENTMAP
	uniform mat3 displacementMapTransform;
	varying vec2 vDisplacementMapUv;
#endif
#ifdef USE_EMISSIVEMAP
	uniform mat3 emissiveMapTransform;
	varying vec2 vEmissiveMapUv;
#endif
#ifdef USE_METALNESSMAP
	uniform mat3 metalnessMapTransform;
	varying vec2 vMetalnessMapUv;
#endif
#ifdef USE_ROUGHNESSMAP
	uniform mat3 roughnessMapTransform;
	varying vec2 vRoughnessMapUv;
#endif
#ifdef USE_ANISOTROPYMAP
	uniform mat3 anisotropyMapTransform;
	varying vec2 vAnisotropyMapUv;
#endif
#ifdef USE_CLEARCOATMAP
	uniform mat3 clearcoatMapTransform;
	varying vec2 vClearcoatMapUv;
#endif
#ifdef USE_CLEARCOAT_NORMALMAP
	uniform mat3 clearcoatNormalMapTransform;
	varying vec2 vClearcoatNormalMapUv;
#endif
#ifdef USE_CLEARCOAT_ROUGHNESSMAP
	uniform mat3 clearcoatRoughnessMapTransform;
	varying vec2 vClearcoatRoughnessMapUv;
#endif
#ifdef USE_SHEEN_COLORMAP
	uniform mat3 sheenColorMapTransform;
	varying vec2 vSheenColorMapUv;
#endif
#ifdef USE_SHEEN_ROUGHNESSMAP
	uniform mat3 sheenRoughnessMapTransform;
	varying vec2 vSheenRoughnessMapUv;
#endif
#ifdef USE_IRIDESCENCEMAP
	uniform mat3 iridescenceMapTransform;
	varying vec2 vIridescenceMapUv;
#endif
#ifdef USE_IRIDESCENCE_THICKNESSMAP
	uniform mat3 iridescenceThicknessMapTransform;
	varying vec2 vIridescenceThicknessMapUv;
#endif
#ifdef USE_SPECULARMAP
	uniform mat3 specularMapTransform;
	varying vec2 vSpecularMapUv;
#endif
#ifdef USE_SPECULAR_COLORMAP
	uniform mat3 specularColorMapTransform;
	varying vec2 vSpecularColorMapUv;
#endif
#ifdef USE_SPECULAR_INTENSITYMAP
	uniform mat3 specularIntensityMapTransform;
	varying vec2 vSpecularIntensityMapUv;
#endif
#ifdef USE_TRANSMISSIONMAP
	uniform mat3 transmissionMapTransform;
	varying vec2 vTransmissionMapUv;
#endif
#ifdef USE_THICKNESSMAP
	uniform mat3 thicknessMapTransform;
	varying vec2 vThicknessMapUv;
#endif`,HA=`#if defined( USE_UV ) || defined( USE_ANISOTROPY )
	vUv = vec3( uv, 1 ).xy;
#endif
#ifdef USE_MAP
	vMapUv = ( mapTransform * vec3( MAP_UV, 1 ) ).xy;
#endif
#ifdef USE_ALPHAMAP
	vAlphaMapUv = ( alphaMapTransform * vec3( ALPHAMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_LIGHTMAP
	vLightMapUv = ( lightMapTransform * vec3( LIGHTMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_AOMAP
	vAoMapUv = ( aoMapTransform * vec3( AOMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_BUMPMAP
	vBumpMapUv = ( bumpMapTransform * vec3( BUMPMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_NORMALMAP
	vNormalMapUv = ( normalMapTransform * vec3( NORMALMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_DISPLACEMENTMAP
	vDisplacementMapUv = ( displacementMapTransform * vec3( DISPLACEMENTMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_EMISSIVEMAP
	vEmissiveMapUv = ( emissiveMapTransform * vec3( EMISSIVEMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_METALNESSMAP
	vMetalnessMapUv = ( metalnessMapTransform * vec3( METALNESSMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_ROUGHNESSMAP
	vRoughnessMapUv = ( roughnessMapTransform * vec3( ROUGHNESSMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_ANISOTROPYMAP
	vAnisotropyMapUv = ( anisotropyMapTransform * vec3( ANISOTROPYMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_CLEARCOATMAP
	vClearcoatMapUv = ( clearcoatMapTransform * vec3( CLEARCOATMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_CLEARCOAT_NORMALMAP
	vClearcoatNormalMapUv = ( clearcoatNormalMapTransform * vec3( CLEARCOAT_NORMALMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_CLEARCOAT_ROUGHNESSMAP
	vClearcoatRoughnessMapUv = ( clearcoatRoughnessMapTransform * vec3( CLEARCOAT_ROUGHNESSMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_IRIDESCENCEMAP
	vIridescenceMapUv = ( iridescenceMapTransform * vec3( IRIDESCENCEMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_IRIDESCENCE_THICKNESSMAP
	vIridescenceThicknessMapUv = ( iridescenceThicknessMapTransform * vec3( IRIDESCENCE_THICKNESSMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_SHEEN_COLORMAP
	vSheenColorMapUv = ( sheenColorMapTransform * vec3( SHEEN_COLORMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_SHEEN_ROUGHNESSMAP
	vSheenRoughnessMapUv = ( sheenRoughnessMapTransform * vec3( SHEEN_ROUGHNESSMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_SPECULARMAP
	vSpecularMapUv = ( specularMapTransform * vec3( SPECULARMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_SPECULAR_COLORMAP
	vSpecularColorMapUv = ( specularColorMapTransform * vec3( SPECULAR_COLORMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_SPECULAR_INTENSITYMAP
	vSpecularIntensityMapUv = ( specularIntensityMapTransform * vec3( SPECULAR_INTENSITYMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_TRANSMISSIONMAP
	vTransmissionMapUv = ( transmissionMapTransform * vec3( TRANSMISSIONMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_THICKNESSMAP
	vThicknessMapUv = ( thicknessMapTransform * vec3( THICKNESSMAP_UV, 1 ) ).xy;
#endif`,zA=`#if defined( USE_ENVMAP ) || defined( DISTANCE ) || defined ( USE_SHADOWMAP ) || defined ( USE_TRANSMISSION ) || NUM_SPOT_LIGHT_COORDS > 0
	vec4 worldPosition = vec4( transformed, 1.0 );
	#ifdef USE_BATCHING
		worldPosition = batchingMatrix * worldPosition;
	#endif
	#ifdef USE_INSTANCING
		worldPosition = instanceMatrix * worldPosition;
	#endif
	worldPosition = modelMatrix * worldPosition;
#endif`,GA=`varying vec2 vUv;
uniform mat3 uvTransform;
void main() {
	vUv = ( uvTransform * vec3( uv, 1 ) ).xy;
	gl_Position = vec4( position.xy, 1.0, 1.0 );
}`,WA=`uniform sampler2D t2D;
uniform float backgroundIntensity;
varying vec2 vUv;
void main() {
	vec4 texColor = texture2D( t2D, vUv );
	#ifdef DECODE_VIDEO_TEXTURE
		texColor = vec4( mix( pow( texColor.rgb * 0.9478672986 + vec3( 0.0521327014 ), vec3( 2.4 ) ), texColor.rgb * 0.0773993808, vec3( lessThanEqual( texColor.rgb, vec3( 0.04045 ) ) ) ), texColor.w );
	#endif
	texColor.rgb *= backgroundIntensity;
	gl_FragColor = texColor;
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
}`,jA=`varying vec3 vWorldDirection;
#include <common>
void main() {
	vWorldDirection = transformDirection( position, modelMatrix );
	#include <begin_vertex>
	#include <project_vertex>
	gl_Position.z = gl_Position.w;
}`,$A=`#ifdef ENVMAP_TYPE_CUBE
	uniform samplerCube envMap;
#elif defined( ENVMAP_TYPE_CUBE_UV )
	uniform sampler2D envMap;
#endif
uniform float flipEnvMap;
uniform float backgroundBlurriness;
uniform float backgroundIntensity;
uniform mat3 backgroundRotation;
varying vec3 vWorldDirection;
#include <cube_uv_reflection_fragment>
void main() {
	#ifdef ENVMAP_TYPE_CUBE
		vec4 texColor = textureCube( envMap, backgroundRotation * vec3( flipEnvMap * vWorldDirection.x, vWorldDirection.yz ) );
	#elif defined( ENVMAP_TYPE_CUBE_UV )
		vec4 texColor = textureCubeUV( envMap, backgroundRotation * vWorldDirection, backgroundBlurriness );
	#else
		vec4 texColor = vec4( 0.0, 0.0, 0.0, 1.0 );
	#endif
	texColor.rgb *= backgroundIntensity;
	gl_FragColor = texColor;
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
}`,qA=`varying vec3 vWorldDirection;
#include <common>
void main() {
	vWorldDirection = transformDirection( position, modelMatrix );
	#include <begin_vertex>
	#include <project_vertex>
	gl_Position.z = gl_Position.w;
}`,XA=`uniform samplerCube tCube;
uniform float tFlip;
uniform float opacity;
varying vec3 vWorldDirection;
void main() {
	vec4 texColor = textureCube( tCube, vec3( tFlip * vWorldDirection.x, vWorldDirection.yz ) );
	gl_FragColor = texColor;
	gl_FragColor.a *= opacity;
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
}`,YA=`#include <common>
#include <batching_pars_vertex>
#include <uv_pars_vertex>
#include <displacementmap_pars_vertex>
#include <morphtarget_pars_vertex>
#include <skinning_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
varying vec2 vHighPrecisionZW;
void main() {
	#include <uv_vertex>
	#include <batching_vertex>
	#include <skinbase_vertex>
	#include <morphinstance_vertex>
	#ifdef USE_DISPLACEMENTMAP
		#include <beginnormal_vertex>
		#include <morphnormal_vertex>
		#include <skinnormal_vertex>
	#endif
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <skinning_vertex>
	#include <displacementmap_vertex>
	#include <project_vertex>
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
	vHighPrecisionZW = gl_Position.zw;
}`,ZA=`#if DEPTH_PACKING == 3200
	uniform float opacity;
#endif
#include <common>
#include <packing>
#include <uv_pars_fragment>
#include <map_pars_fragment>
#include <alphamap_pars_fragment>
#include <alphatest_pars_fragment>
#include <alphahash_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
varying vec2 vHighPrecisionZW;
void main() {
	vec4 diffuseColor = vec4( 1.0 );
	#include <clipping_planes_fragment>
	#if DEPTH_PACKING == 3200
		diffuseColor.a = opacity;
	#endif
	#include <map_fragment>
	#include <alphamap_fragment>
	#include <alphatest_fragment>
	#include <alphahash_fragment>
	#include <logdepthbuf_fragment>
	#ifdef USE_REVERSED_DEPTH_BUFFER
		float fragCoordZ = vHighPrecisionZW[ 0 ] / vHighPrecisionZW[ 1 ];
	#else
		float fragCoordZ = 0.5 * vHighPrecisionZW[ 0 ] / vHighPrecisionZW[ 1 ] + 0.5;
	#endif
	#if DEPTH_PACKING == 3200
		gl_FragColor = vec4( vec3( 1.0 - fragCoordZ ), opacity );
	#elif DEPTH_PACKING == 3201
		gl_FragColor = packDepthToRGBA( fragCoordZ );
	#elif DEPTH_PACKING == 3202
		gl_FragColor = vec4( packDepthToRGB( fragCoordZ ), 1.0 );
	#elif DEPTH_PACKING == 3203
		gl_FragColor = vec4( packDepthToRG( fragCoordZ ), 0.0, 1.0 );
	#endif
}`,JA=`#define DISTANCE
varying vec3 vWorldPosition;
#include <common>
#include <batching_pars_vertex>
#include <uv_pars_vertex>
#include <displacementmap_pars_vertex>
#include <morphtarget_pars_vertex>
#include <skinning_pars_vertex>
#include <clipping_planes_pars_vertex>
void main() {
	#include <uv_vertex>
	#include <batching_vertex>
	#include <skinbase_vertex>
	#include <morphinstance_vertex>
	#ifdef USE_DISPLACEMENTMAP
		#include <beginnormal_vertex>
		#include <morphnormal_vertex>
		#include <skinnormal_vertex>
	#endif
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <skinning_vertex>
	#include <displacementmap_vertex>
	#include <project_vertex>
	#include <worldpos_vertex>
	#include <clipping_planes_vertex>
	vWorldPosition = worldPosition.xyz;
}`,KA=`#define DISTANCE
uniform vec3 referencePosition;
uniform float nearDistance;
uniform float farDistance;
varying vec3 vWorldPosition;
#include <common>
#include <uv_pars_fragment>
#include <map_pars_fragment>
#include <alphamap_pars_fragment>
#include <alphatest_pars_fragment>
#include <alphahash_pars_fragment>
#include <clipping_planes_pars_fragment>
void main () {
	vec4 diffuseColor = vec4( 1.0 );
	#include <clipping_planes_fragment>
	#include <map_fragment>
	#include <alphamap_fragment>
	#include <alphatest_fragment>
	#include <alphahash_fragment>
	float dist = length( vWorldPosition - referencePosition );
	dist = ( dist - nearDistance ) / ( farDistance - nearDistance );
	dist = saturate( dist );
	gl_FragColor = vec4( dist, 0.0, 0.0, 1.0 );
}`,QA=`varying vec3 vWorldDirection;
#include <common>
void main() {
	vWorldDirection = transformDirection( position, modelMatrix );
	#include <begin_vertex>
	#include <project_vertex>
}`,eD=`uniform sampler2D tEquirect;
varying vec3 vWorldDirection;
#include <common>
void main() {
	vec3 direction = normalize( vWorldDirection );
	vec2 sampleUV = equirectUv( direction );
	gl_FragColor = texture2D( tEquirect, sampleUV );
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
}`,tD=`uniform float scale;
attribute float lineDistance;
varying float vLineDistance;
#include <common>
#include <uv_pars_vertex>
#include <color_pars_vertex>
#include <fog_pars_vertex>
#include <morphtarget_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
void main() {
	vLineDistance = scale * lineDistance;
	#include <uv_vertex>
	#include <color_vertex>
	#include <morphinstance_vertex>
	#include <morphcolor_vertex>
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <project_vertex>
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
	#include <fog_vertex>
}`,nD=`uniform vec3 diffuse;
uniform float opacity;
uniform float dashSize;
uniform float totalSize;
varying float vLineDistance;
#include <common>
#include <color_pars_fragment>
#include <uv_pars_fragment>
#include <map_pars_fragment>
#include <fog_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( diffuse, opacity );
	#include <clipping_planes_fragment>
	if ( mod( vLineDistance, totalSize ) > dashSize ) {
		discard;
	}
	vec3 outgoingLight = vec3( 0.0 );
	#include <logdepthbuf_fragment>
	#include <map_fragment>
	#include <color_fragment>
	outgoingLight = diffuseColor.rgb;
	#include <opaque_fragment>
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
	#include <fog_fragment>
	#include <premultiplied_alpha_fragment>
}`,iD=`#include <common>
#include <batching_pars_vertex>
#include <uv_pars_vertex>
#include <envmap_pars_vertex>
#include <color_pars_vertex>
#include <fog_pars_vertex>
#include <morphtarget_pars_vertex>
#include <skinning_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
void main() {
	#include <uv_vertex>
	#include <color_vertex>
	#include <morphinstance_vertex>
	#include <morphcolor_vertex>
	#include <batching_vertex>
	#if defined ( USE_ENVMAP ) || defined ( USE_SKINNING )
		#include <beginnormal_vertex>
		#include <morphnormal_vertex>
		#include <skinbase_vertex>
		#include <skinnormal_vertex>
		#include <defaultnormal_vertex>
	#endif
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <skinning_vertex>
	#include <project_vertex>
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
	#include <worldpos_vertex>
	#include <envmap_vertex>
	#include <fog_vertex>
}`,rD=`uniform vec3 diffuse;
uniform float opacity;
#ifndef FLAT_SHADED
	varying vec3 vNormal;
#endif
#include <common>
#include <dithering_pars_fragment>
#include <color_pars_fragment>
#include <uv_pars_fragment>
#include <map_pars_fragment>
#include <alphamap_pars_fragment>
#include <alphatest_pars_fragment>
#include <alphahash_pars_fragment>
#include <aomap_pars_fragment>
#include <lightmap_pars_fragment>
#include <envmap_common_pars_fragment>
#include <envmap_pars_fragment>
#include <fog_pars_fragment>
#include <specularmap_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( diffuse, opacity );
	#include <clipping_planes_fragment>
	#include <logdepthbuf_fragment>
	#include <map_fragment>
	#include <color_fragment>
	#include <alphamap_fragment>
	#include <alphatest_fragment>
	#include <alphahash_fragment>
	#include <specularmap_fragment>
	ReflectedLight reflectedLight = ReflectedLight( vec3( 0.0 ), vec3( 0.0 ), vec3( 0.0 ), vec3( 0.0 ) );
	#ifdef USE_LIGHTMAP
		vec4 lightMapTexel = texture2D( lightMap, vLightMapUv );
		reflectedLight.indirectDiffuse += lightMapTexel.rgb * lightMapIntensity * RECIPROCAL_PI;
	#else
		reflectedLight.indirectDiffuse += vec3( 1.0 );
	#endif
	#include <aomap_fragment>
	reflectedLight.indirectDiffuse *= diffuseColor.rgb;
	vec3 outgoingLight = reflectedLight.indirectDiffuse;
	#include <envmap_fragment>
	#include <opaque_fragment>
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
	#include <fog_fragment>
	#include <premultiplied_alpha_fragment>
	#include <dithering_fragment>
}`,sD=`#define LAMBERT
varying vec3 vViewPosition;
#include <common>
#include <batching_pars_vertex>
#include <uv_pars_vertex>
#include <displacementmap_pars_vertex>
#include <envmap_pars_vertex>
#include <color_pars_vertex>
#include <fog_pars_vertex>
#include <normal_pars_vertex>
#include <morphtarget_pars_vertex>
#include <skinning_pars_vertex>
#include <shadowmap_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
void main() {
	#include <uv_vertex>
	#include <color_vertex>
	#include <morphinstance_vertex>
	#include <morphcolor_vertex>
	#include <batching_vertex>
	#include <beginnormal_vertex>
	#include <morphnormal_vertex>
	#include <skinbase_vertex>
	#include <skinnormal_vertex>
	#include <defaultnormal_vertex>
	#include <normal_vertex>
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <skinning_vertex>
	#include <displacementmap_vertex>
	#include <project_vertex>
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
	vViewPosition = - mvPosition.xyz;
	#include <worldpos_vertex>
	#include <envmap_vertex>
	#include <shadowmap_vertex>
	#include <fog_vertex>
}`,oD=`#define LAMBERT
uniform vec3 diffuse;
uniform vec3 emissive;
uniform float opacity;
#include <common>
#include <dithering_pars_fragment>
#include <color_pars_fragment>
#include <uv_pars_fragment>
#include <map_pars_fragment>
#include <alphamap_pars_fragment>
#include <alphatest_pars_fragment>
#include <alphahash_pars_fragment>
#include <aomap_pars_fragment>
#include <lightmap_pars_fragment>
#include <emissivemap_pars_fragment>
#include <cube_uv_reflection_fragment>
#include <envmap_common_pars_fragment>
#include <envmap_pars_fragment>
#include <envmap_physical_pars_fragment>
#include <fog_pars_fragment>
#include <bsdfs>
#include <lights_pars_begin>
#include <normal_pars_fragment>
#include <lights_lambert_pars_fragment>
#include <shadowmap_pars_fragment>
#include <bumpmap_pars_fragment>
#include <normalmap_pars_fragment>
#include <specularmap_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( diffuse, opacity );
	#include <clipping_planes_fragment>
	ReflectedLight reflectedLight = ReflectedLight( vec3( 0.0 ), vec3( 0.0 ), vec3( 0.0 ), vec3( 0.0 ) );
	vec3 totalEmissiveRadiance = emissive;
	#include <logdepthbuf_fragment>
	#include <map_fragment>
	#include <color_fragment>
	#include <alphamap_fragment>
	#include <alphatest_fragment>
	#include <alphahash_fragment>
	#include <specularmap_fragment>
	#include <normal_fragment_begin>
	#include <normal_fragment_maps>
	#include <emissivemap_fragment>
	#include <lights_lambert_fragment>
	#include <lights_fragment_begin>
	#include <lights_fragment_maps>
	#include <lights_fragment_end>
	#include <aomap_fragment>
	vec3 outgoingLight = reflectedLight.directDiffuse + reflectedLight.indirectDiffuse + totalEmissiveRadiance;
	#include <envmap_fragment>
	#include <opaque_fragment>
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
	#include <fog_fragment>
	#include <premultiplied_alpha_fragment>
	#include <dithering_fragment>
}`,aD=`#define MATCAP
varying vec3 vViewPosition;
#include <common>
#include <batching_pars_vertex>
#include <uv_pars_vertex>
#include <color_pars_vertex>
#include <displacementmap_pars_vertex>
#include <fog_pars_vertex>
#include <normal_pars_vertex>
#include <morphtarget_pars_vertex>
#include <skinning_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
void main() {
	#include <uv_vertex>
	#include <color_vertex>
	#include <morphinstance_vertex>
	#include <morphcolor_vertex>
	#include <batching_vertex>
	#include <beginnormal_vertex>
	#include <morphnormal_vertex>
	#include <skinbase_vertex>
	#include <skinnormal_vertex>
	#include <defaultnormal_vertex>
	#include <normal_vertex>
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <skinning_vertex>
	#include <displacementmap_vertex>
	#include <project_vertex>
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
	#include <fog_vertex>
	vViewPosition = - mvPosition.xyz;
}`,cD=`#define MATCAP
uniform vec3 diffuse;
uniform float opacity;
uniform sampler2D matcap;
varying vec3 vViewPosition;
#include <common>
#include <dithering_pars_fragment>
#include <color_pars_fragment>
#include <uv_pars_fragment>
#include <map_pars_fragment>
#include <alphamap_pars_fragment>
#include <alphatest_pars_fragment>
#include <alphahash_pars_fragment>
#include <fog_pars_fragment>
#include <normal_pars_fragment>
#include <bumpmap_pars_fragment>
#include <normalmap_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( diffuse, opacity );
	#include <clipping_planes_fragment>
	#include <logdepthbuf_fragment>
	#include <map_fragment>
	#include <color_fragment>
	#include <alphamap_fragment>
	#include <alphatest_fragment>
	#include <alphahash_fragment>
	#include <normal_fragment_begin>
	#include <normal_fragment_maps>
	vec3 viewDir = normalize( vViewPosition );
	vec3 x = normalize( vec3( viewDir.z, 0.0, - viewDir.x ) );
	vec3 y = cross( viewDir, x );
	vec2 uv = vec2( dot( x, normal ), dot( y, normal ) ) * 0.495 + 0.5;
	#ifdef USE_MATCAP
		vec4 matcapColor = texture2D( matcap, uv );
	#else
		vec4 matcapColor = vec4( vec3( mix( 0.2, 0.8, uv.y ) ), 1.0 );
	#endif
	vec3 outgoingLight = diffuseColor.rgb * matcapColor.rgb;
	#include <opaque_fragment>
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
	#include <fog_fragment>
	#include <premultiplied_alpha_fragment>
	#include <dithering_fragment>
}`,lD=`#define NORMAL
#if defined( FLAT_SHADED ) || defined( USE_BUMPMAP ) || defined( USE_NORMALMAP_TANGENTSPACE )
	varying vec3 vViewPosition;
#endif
#include <common>
#include <batching_pars_vertex>
#include <uv_pars_vertex>
#include <displacementmap_pars_vertex>
#include <normal_pars_vertex>
#include <morphtarget_pars_vertex>
#include <skinning_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
void main() {
	#include <uv_vertex>
	#include <batching_vertex>
	#include <beginnormal_vertex>
	#include <morphinstance_vertex>
	#include <morphnormal_vertex>
	#include <skinbase_vertex>
	#include <skinnormal_vertex>
	#include <defaultnormal_vertex>
	#include <normal_vertex>
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <skinning_vertex>
	#include <displacementmap_vertex>
	#include <project_vertex>
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
#if defined( FLAT_SHADED ) || defined( USE_BUMPMAP ) || defined( USE_NORMALMAP_TANGENTSPACE )
	vViewPosition = - mvPosition.xyz;
#endif
}`,uD=`#define NORMAL
uniform float opacity;
#if defined( FLAT_SHADED ) || defined( USE_BUMPMAP ) || defined( USE_NORMALMAP_TANGENTSPACE )
	varying vec3 vViewPosition;
#endif
#include <uv_pars_fragment>
#include <normal_pars_fragment>
#include <bumpmap_pars_fragment>
#include <normalmap_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( 0.0, 0.0, 0.0, opacity );
	#include <clipping_planes_fragment>
	#include <logdepthbuf_fragment>
	#include <normal_fragment_begin>
	#include <normal_fragment_maps>
	gl_FragColor = vec4( normalize( normal ) * 0.5 + 0.5, diffuseColor.a );
	#ifdef OPAQUE
		gl_FragColor.a = 1.0;
	#endif
}`,dD=`#define PHONG
varying vec3 vViewPosition;
#include <common>
#include <batching_pars_vertex>
#include <uv_pars_vertex>
#include <displacementmap_pars_vertex>
#include <envmap_pars_vertex>
#include <color_pars_vertex>
#include <fog_pars_vertex>
#include <normal_pars_vertex>
#include <morphtarget_pars_vertex>
#include <skinning_pars_vertex>
#include <shadowmap_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
void main() {
	#include <uv_vertex>
	#include <color_vertex>
	#include <morphcolor_vertex>
	#include <batching_vertex>
	#include <beginnormal_vertex>
	#include <morphinstance_vertex>
	#include <morphnormal_vertex>
	#include <skinbase_vertex>
	#include <skinnormal_vertex>
	#include <defaultnormal_vertex>
	#include <normal_vertex>
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <skinning_vertex>
	#include <displacementmap_vertex>
	#include <project_vertex>
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
	vViewPosition = - mvPosition.xyz;
	#include <worldpos_vertex>
	#include <envmap_vertex>
	#include <shadowmap_vertex>
	#include <fog_vertex>
}`,fD=`#define PHONG
uniform vec3 diffuse;
uniform vec3 emissive;
uniform vec3 specular;
uniform float shininess;
uniform float opacity;
#include <common>
#include <dithering_pars_fragment>
#include <color_pars_fragment>
#include <uv_pars_fragment>
#include <map_pars_fragment>
#include <alphamap_pars_fragment>
#include <alphatest_pars_fragment>
#include <alphahash_pars_fragment>
#include <aomap_pars_fragment>
#include <lightmap_pars_fragment>
#include <emissivemap_pars_fragment>
#include <cube_uv_reflection_fragment>
#include <envmap_common_pars_fragment>
#include <envmap_pars_fragment>
#include <envmap_physical_pars_fragment>
#include <fog_pars_fragment>
#include <bsdfs>
#include <lights_pars_begin>
#include <normal_pars_fragment>
#include <lights_phong_pars_fragment>
#include <shadowmap_pars_fragment>
#include <bumpmap_pars_fragment>
#include <normalmap_pars_fragment>
#include <specularmap_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( diffuse, opacity );
	#include <clipping_planes_fragment>
	ReflectedLight reflectedLight = ReflectedLight( vec3( 0.0 ), vec3( 0.0 ), vec3( 0.0 ), vec3( 0.0 ) );
	vec3 totalEmissiveRadiance = emissive;
	#include <logdepthbuf_fragment>
	#include <map_fragment>
	#include <color_fragment>
	#include <alphamap_fragment>
	#include <alphatest_fragment>
	#include <alphahash_fragment>
	#include <specularmap_fragment>
	#include <normal_fragment_begin>
	#include <normal_fragment_maps>
	#include <emissivemap_fragment>
	#include <lights_phong_fragment>
	#include <lights_fragment_begin>
	#include <lights_fragment_maps>
	#include <lights_fragment_end>
	#include <aomap_fragment>
	vec3 outgoingLight = reflectedLight.directDiffuse + reflectedLight.indirectDiffuse + reflectedLight.directSpecular + reflectedLight.indirectSpecular + totalEmissiveRadiance;
	#include <envmap_fragment>
	#include <opaque_fragment>
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
	#include <fog_fragment>
	#include <premultiplied_alpha_fragment>
	#include <dithering_fragment>
}`,hD=`#define STANDARD
varying vec3 vViewPosition;
#ifdef USE_TRANSMISSION
	varying vec3 vWorldPosition;
#endif
#include <common>
#include <batching_pars_vertex>
#include <uv_pars_vertex>
#include <displacementmap_pars_vertex>
#include <color_pars_vertex>
#include <fog_pars_vertex>
#include <normal_pars_vertex>
#include <morphtarget_pars_vertex>
#include <skinning_pars_vertex>
#include <shadowmap_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
void main() {
	#include <uv_vertex>
	#include <color_vertex>
	#include <morphinstance_vertex>
	#include <morphcolor_vertex>
	#include <batching_vertex>
	#include <beginnormal_vertex>
	#include <morphnormal_vertex>
	#include <skinbase_vertex>
	#include <skinnormal_vertex>
	#include <defaultnormal_vertex>
	#include <normal_vertex>
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <skinning_vertex>
	#include <displacementmap_vertex>
	#include <project_vertex>
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
	vViewPosition = - mvPosition.xyz;
	#include <worldpos_vertex>
	#include <shadowmap_vertex>
	#include <fog_vertex>
#ifdef USE_TRANSMISSION
	vWorldPosition = worldPosition.xyz;
#endif
}`,pD=`#define STANDARD
#ifdef PHYSICAL
	#define IOR
	#define USE_SPECULAR
#endif
uniform vec3 diffuse;
uniform vec3 emissive;
uniform float roughness;
uniform float metalness;
uniform float opacity;
#ifdef IOR
	uniform float ior;
#endif
#ifdef USE_SPECULAR
	uniform float specularIntensity;
	uniform vec3 specularColor;
	#ifdef USE_SPECULAR_COLORMAP
		uniform sampler2D specularColorMap;
	#endif
	#ifdef USE_SPECULAR_INTENSITYMAP
		uniform sampler2D specularIntensityMap;
	#endif
#endif
#ifdef USE_CLEARCOAT
	uniform float clearcoat;
	uniform float clearcoatRoughness;
#endif
#ifdef USE_DISPERSION
	uniform float dispersion;
#endif
#ifdef USE_IRIDESCENCE
	uniform float iridescence;
	uniform float iridescenceIOR;
	uniform float iridescenceThicknessMinimum;
	uniform float iridescenceThicknessMaximum;
#endif
#ifdef USE_SHEEN
	uniform vec3 sheenColor;
	uniform float sheenRoughness;
	#ifdef USE_SHEEN_COLORMAP
		uniform sampler2D sheenColorMap;
	#endif
	#ifdef USE_SHEEN_ROUGHNESSMAP
		uniform sampler2D sheenRoughnessMap;
	#endif
#endif
#ifdef USE_ANISOTROPY
	uniform vec2 anisotropyVector;
	#ifdef USE_ANISOTROPYMAP
		uniform sampler2D anisotropyMap;
	#endif
#endif
varying vec3 vViewPosition;
#include <common>
#include <dithering_pars_fragment>
#include <color_pars_fragment>
#include <uv_pars_fragment>
#include <map_pars_fragment>
#include <alphamap_pars_fragment>
#include <alphatest_pars_fragment>
#include <alphahash_pars_fragment>
#include <aomap_pars_fragment>
#include <lightmap_pars_fragment>
#include <emissivemap_pars_fragment>
#include <iridescence_fragment>
#include <cube_uv_reflection_fragment>
#include <envmap_common_pars_fragment>
#include <envmap_physical_pars_fragment>
#include <fog_pars_fragment>
#include <lights_pars_begin>
#include <normal_pars_fragment>
#include <lights_physical_pars_fragment>
#include <transmission_pars_fragment>
#include <shadowmap_pars_fragment>
#include <bumpmap_pars_fragment>
#include <normalmap_pars_fragment>
#include <clearcoat_pars_fragment>
#include <iridescence_pars_fragment>
#include <roughnessmap_pars_fragment>
#include <metalnessmap_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( diffuse, opacity );
	#include <clipping_planes_fragment>
	ReflectedLight reflectedLight = ReflectedLight( vec3( 0.0 ), vec3( 0.0 ), vec3( 0.0 ), vec3( 0.0 ) );
	vec3 totalEmissiveRadiance = emissive;
	#include <logdepthbuf_fragment>
	#include <map_fragment>
	#include <color_fragment>
	#include <alphamap_fragment>
	#include <alphatest_fragment>
	#include <alphahash_fragment>
	#include <roughnessmap_fragment>
	#include <metalnessmap_fragment>
	#include <normal_fragment_begin>
	#include <normal_fragment_maps>
	#include <clearcoat_normal_fragment_begin>
	#include <clearcoat_normal_fragment_maps>
	#include <emissivemap_fragment>
	#include <lights_physical_fragment>
	#include <lights_fragment_begin>
	#include <lights_fragment_maps>
	#include <lights_fragment_end>
	#include <aomap_fragment>
	vec3 totalDiffuse = reflectedLight.directDiffuse + reflectedLight.indirectDiffuse;
	vec3 totalSpecular = reflectedLight.directSpecular + reflectedLight.indirectSpecular;
	#include <transmission_fragment>
	vec3 outgoingLight = totalDiffuse + totalSpecular + totalEmissiveRadiance;
	#ifdef USE_SHEEN
 
		outgoingLight = outgoingLight + sheenSpecularDirect + sheenSpecularIndirect;
 
 	#endif
	#ifdef USE_CLEARCOAT
		float dotNVcc = saturate( dot( geometryClearcoatNormal, geometryViewDir ) );
		vec3 Fcc = F_Schlick( material.clearcoatF0, material.clearcoatF90, dotNVcc );
		outgoingLight = outgoingLight * ( 1.0 - material.clearcoat * Fcc ) + ( clearcoatSpecularDirect + clearcoatSpecularIndirect ) * material.clearcoat;
	#endif
	#include <opaque_fragment>
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
	#include <fog_fragment>
	#include <premultiplied_alpha_fragment>
	#include <dithering_fragment>
}`,mD=`#define TOON
varying vec3 vViewPosition;
#include <common>
#include <batching_pars_vertex>
#include <uv_pars_vertex>
#include <displacementmap_pars_vertex>
#include <color_pars_vertex>
#include <fog_pars_vertex>
#include <normal_pars_vertex>
#include <morphtarget_pars_vertex>
#include <skinning_pars_vertex>
#include <shadowmap_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
void main() {
	#include <uv_vertex>
	#include <color_vertex>
	#include <morphinstance_vertex>
	#include <morphcolor_vertex>
	#include <batching_vertex>
	#include <beginnormal_vertex>
	#include <morphnormal_vertex>
	#include <skinbase_vertex>
	#include <skinnormal_vertex>
	#include <defaultnormal_vertex>
	#include <normal_vertex>
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <skinning_vertex>
	#include <displacementmap_vertex>
	#include <project_vertex>
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
	vViewPosition = - mvPosition.xyz;
	#include <worldpos_vertex>
	#include <shadowmap_vertex>
	#include <fog_vertex>
}`,gD=`#define TOON
uniform vec3 diffuse;
uniform vec3 emissive;
uniform float opacity;
#include <common>
#include <dithering_pars_fragment>
#include <color_pars_fragment>
#include <uv_pars_fragment>
#include <map_pars_fragment>
#include <alphamap_pars_fragment>
#include <alphatest_pars_fragment>
#include <alphahash_pars_fragment>
#include <aomap_pars_fragment>
#include <lightmap_pars_fragment>
#include <emissivemap_pars_fragment>
#include <gradientmap_pars_fragment>
#include <fog_pars_fragment>
#include <bsdfs>
#include <lights_pars_begin>
#include <normal_pars_fragment>
#include <lights_toon_pars_fragment>
#include <shadowmap_pars_fragment>
#include <bumpmap_pars_fragment>
#include <normalmap_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( diffuse, opacity );
	#include <clipping_planes_fragment>
	ReflectedLight reflectedLight = ReflectedLight( vec3( 0.0 ), vec3( 0.0 ), vec3( 0.0 ), vec3( 0.0 ) );
	vec3 totalEmissiveRadiance = emissive;
	#include <logdepthbuf_fragment>
	#include <map_fragment>
	#include <color_fragment>
	#include <alphamap_fragment>
	#include <alphatest_fragment>
	#include <alphahash_fragment>
	#include <normal_fragment_begin>
	#include <normal_fragment_maps>
	#include <emissivemap_fragment>
	#include <lights_toon_fragment>
	#include <lights_fragment_begin>
	#include <lights_fragment_maps>
	#include <lights_fragment_end>
	#include <aomap_fragment>
	vec3 outgoingLight = reflectedLight.directDiffuse + reflectedLight.indirectDiffuse + totalEmissiveRadiance;
	#include <opaque_fragment>
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
	#include <fog_fragment>
	#include <premultiplied_alpha_fragment>
	#include <dithering_fragment>
}`,vD=`uniform float size;
uniform float scale;
#include <common>
#include <color_pars_vertex>
#include <fog_pars_vertex>
#include <morphtarget_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
#ifdef USE_POINTS_UV
	varying vec2 vUv;
	uniform mat3 uvTransform;
#endif
void main() {
	#ifdef USE_POINTS_UV
		vUv = ( uvTransform * vec3( uv, 1 ) ).xy;
	#endif
	#include <color_vertex>
	#include <morphinstance_vertex>
	#include <morphcolor_vertex>
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <project_vertex>
	gl_PointSize = size;
	#ifdef USE_SIZEATTENUATION
		bool isPerspective = isPerspectiveMatrix( projectionMatrix );
		if ( isPerspective ) gl_PointSize *= ( scale / - mvPosition.z );
	#endif
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
	#include <worldpos_vertex>
	#include <fog_vertex>
}`,yD=`uniform vec3 diffuse;
uniform float opacity;
#include <common>
#include <color_pars_fragment>
#include <map_particle_pars_fragment>
#include <alphatest_pars_fragment>
#include <alphahash_pars_fragment>
#include <fog_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( diffuse, opacity );
	#include <clipping_planes_fragment>
	vec3 outgoingLight = vec3( 0.0 );
	#include <logdepthbuf_fragment>
	#include <map_particle_fragment>
	#include <color_fragment>
	#include <alphatest_fragment>
	#include <alphahash_fragment>
	outgoingLight = diffuseColor.rgb;
	#include <opaque_fragment>
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
	#include <fog_fragment>
	#include <premultiplied_alpha_fragment>
}`,_D=`#include <common>
#include <batching_pars_vertex>
#include <fog_pars_vertex>
#include <morphtarget_pars_vertex>
#include <skinning_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <shadowmap_pars_vertex>
void main() {
	#include <batching_vertex>
	#include <beginnormal_vertex>
	#include <morphinstance_vertex>
	#include <morphnormal_vertex>
	#include <skinbase_vertex>
	#include <skinnormal_vertex>
	#include <defaultnormal_vertex>
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <skinning_vertex>
	#include <project_vertex>
	#include <logdepthbuf_vertex>
	#include <worldpos_vertex>
	#include <shadowmap_vertex>
	#include <fog_vertex>
}`,xD=`uniform vec3 color;
uniform float opacity;
#include <common>
#include <fog_pars_fragment>
#include <bsdfs>
#include <lights_pars_begin>
#include <logdepthbuf_pars_fragment>
#include <shadowmap_pars_fragment>
#include <shadowmask_pars_fragment>
void main() {
	#include <logdepthbuf_fragment>
	gl_FragColor = vec4( color, opacity * ( 1.0 - getShadowMask() ) );
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
	#include <fog_fragment>
	#include <premultiplied_alpha_fragment>
}`,MD=`uniform float rotation;
uniform vec2 center;
#include <common>
#include <uv_pars_vertex>
#include <fog_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
void main() {
	#include <uv_vertex>
	vec4 mvPosition = modelViewMatrix[ 3 ];
	vec2 scale = vec2( length( modelMatrix[ 0 ].xyz ), length( modelMatrix[ 1 ].xyz ) );
	#ifndef USE_SIZEATTENUATION
		bool isPerspective = isPerspectiveMatrix( projectionMatrix );
		if ( isPerspective ) scale *= - mvPosition.z;
	#endif
	vec2 alignedPosition = ( position.xy - ( center - vec2( 0.5 ) ) ) * scale;
	vec2 rotatedPosition;
	rotatedPosition.x = cos( rotation ) * alignedPosition.x - sin( rotation ) * alignedPosition.y;
	rotatedPosition.y = sin( rotation ) * alignedPosition.x + cos( rotation ) * alignedPosition.y;
	mvPosition.xy += rotatedPosition;
	gl_Position = projectionMatrix * mvPosition;
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
	#include <fog_vertex>
}`,bD=`uniform vec3 diffuse;
uniform float opacity;
#include <common>
#include <uv_pars_fragment>
#include <map_pars_fragment>
#include <alphamap_pars_fragment>
#include <alphatest_pars_fragment>
#include <alphahash_pars_fragment>
#include <fog_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( diffuse, opacity );
	#include <clipping_planes_fragment>
	vec3 outgoingLight = vec3( 0.0 );
	#include <logdepthbuf_fragment>
	#include <map_fragment>
	#include <alphamap_fragment>
	#include <alphatest_fragment>
	#include <alphahash_fragment>
	outgoingLight = diffuseColor.rgb;
	#include <opaque_fragment>
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
	#include <fog_fragment>
}`,ze={alphahash_fragment:WC,alphahash_pars_fragment:jC,alphamap_fragment:$C,alphamap_pars_fragment:qC,alphatest_fragment:XC,alphatest_pars_fragment:YC,aomap_fragment:ZC,aomap_pars_fragment:JC,batching_pars_vertex:KC,batching_vertex:QC,begin_vertex:eI,beginnormal_vertex:tI,bsdfs:nI,iridescence_fragment:iI,bumpmap_pars_fragment:rI,clipping_planes_fragment:sI,clipping_planes_pars_fragment:oI,clipping_planes_pars_vertex:aI,clipping_planes_vertex:cI,color_fragment:lI,color_pars_fragment:uI,color_pars_vertex:dI,color_vertex:fI,common:hI,cube_uv_reflection_fragment:pI,defaultnormal_vertex:mI,displacementmap_pars_vertex:gI,displacementmap_vertex:vI,emissivemap_fragment:yI,emissivemap_pars_fragment:_I,colorspace_fragment:xI,colorspace_pars_fragment:MI,envmap_fragment:bI,envmap_common_pars_fragment:SI,envmap_pars_fragment:EI,envmap_pars_vertex:TI,envmap_physical_pars_fragment:FI,envmap_vertex:wI,fog_vertex:CI,fog_pars_vertex:II,fog_fragment:AI,fog_pars_fragment:DI,gradientmap_pars_fragment:RI,lightmap_pars_fragment:NI,lights_lambert_fragment:PI,lights_lambert_pars_fragment:LI,lights_pars_begin:OI,lights_toon_fragment:kI,lights_toon_pars_fragment:UI,lights_phong_fragment:BI,lights_phong_pars_fragment:VI,lights_physical_fragment:HI,lights_physical_pars_fragment:zI,lights_fragment_begin:GI,lights_fragment_maps:WI,lights_fragment_end:jI,logdepthbuf_fragment:$I,logdepthbuf_pars_fragment:qI,logdepthbuf_pars_vertex:XI,logdepthbuf_vertex:YI,map_fragment:ZI,map_pars_fragment:JI,map_particle_fragment:KI,map_particle_pars_fragment:QI,metalnessmap_fragment:eA,metalnessmap_pars_fragment:tA,morphinstance_vertex:nA,morphcolor_vertex:iA,morphnormal_vertex:rA,morphtarget_pars_vertex:sA,morphtarget_vertex:oA,normal_fragment_begin:aA,normal_fragment_maps:cA,normal_pars_fragment:lA,normal_pars_vertex:uA,normal_vertex:dA,normalmap_pars_fragment:fA,clearcoat_normal_fragment_begin:hA,clearcoat_normal_fragment_maps:pA,clearcoat_pars_fragment:mA,iridescence_pars_fragment:gA,opaque_fragment:vA,packing:yA,premultiplied_alpha_fragment:_A,project_vertex:xA,dithering_fragment:MA,dithering_pars_fragment:bA,roughnessmap_fragment:SA,roughnessmap_pars_fragment:EA,shadowmap_pars_fragment:TA,shadowmap_pars_vertex:wA,shadowmap_vertex:CA,shadowmask_pars_fragment:IA,skinbase_vertex:AA,skinning_pars_vertex:DA,skinning_vertex:RA,skinnormal_vertex:NA,specularmap_fragment:PA,specularmap_pars_fragment:LA,tonemapping_fragment:OA,tonemapping_pars_fragment:FA,transmission_fragment:kA,transmission_pars_fragment:UA,uv_pars_fragment:BA,uv_pars_vertex:VA,uv_vertex:HA,worldpos_vertex:zA,background_vert:GA,background_frag:WA,backgroundCube_vert:jA,backgroundCube_frag:$A,cube_vert:qA,cube_frag:XA,depth_vert:YA,depth_frag:ZA,distance_vert:JA,distance_frag:KA,equirect_vert:QA,equirect_frag:eD,linedashed_vert:tD,linedashed_frag:nD,meshbasic_vert:iD,meshbasic_frag:rD,meshlambert_vert:sD,meshlambert_frag:oD,meshmatcap_vert:aD,meshmatcap_frag:cD,meshnormal_vert:lD,meshnormal_frag:uD,meshphong_vert:dD,meshphong_frag:fD,meshphysical_vert:hD,meshphysical_frag:pD,meshtoon_vert:mD,meshtoon_frag:gD,points_vert:vD,points_frag:yD,shadow_vert:_D,shadow_frag:xD,sprite_vert:MD,sprite_frag:bD},oe={common:{diffuse:{value:new Xe(16777215)},opacity:{value:1},map:{value:null},mapTransform:{value:new Be},alphaMap:{value:null},alphaMapTransform:{value:new Be},alphaTest:{value:0}},specularmap:{specularMap:{value:null},specularMapTransform:{value:new Be}},envmap:{envMap:{value:null},envMapRotation:{value:new Be},flipEnvMap:{value:-1},reflectivity:{value:1},ior:{value:1.5},refractionRatio:{value:.98},dfgLUT:{value:null}},aomap:{aoMap:{value:null},aoMapIntensity:{value:1},aoMapTransform:{value:new Be}},lightmap:{lightMap:{value:null},lightMapIntensity:{value:1},lightMapTransform:{value:new Be}},bumpmap:{bumpMap:{value:null},bumpMapTransform:{value:new Be},bumpScale:{value:1}},normalmap:{normalMap:{value:null},normalMapTransform:{value:new Be},normalScale:{value:new ht(1,1)}},displacementmap:{displacementMap:{value:null},displacementMapTransform:{value:new Be},displacementScale:{value:1},displacementBias:{value:0}},emissivemap:{emissiveMap:{value:null},emissiveMapTransform:{value:new Be}},metalnessmap:{metalnessMap:{value:null},metalnessMapTransform:{value:new Be}},roughnessmap:{roughnessMap:{value:null},roughnessMapTransform:{value:new Be}},gradientmap:{gradientMap:{value:null}},fog:{fogDensity:{value:25e-5},fogNear:{value:1},fogFar:{value:2e3},fogColor:{value:new Xe(16777215)}},lights:{ambientLightColor:{value:[]},lightProbe:{value:[]},directionalLights:{value:[],properties:{direction:{},color:{}}},directionalLightShadows:{value:[],properties:{shadowIntensity:1,shadowBias:{},shadowNormalBias:{},shadowRadius:{},shadowMapSize:{}}},directionalShadowMatrix:{value:[]},spotLights:{value:[],properties:{color:{},position:{},direction:{},distance:{},coneCos:{},penumbraCos:{},decay:{}}},spotLightShadows:{value:[],properties:{shadowIntensity:1,shadowBias:{},shadowNormalBias:{},shadowRadius:{},shadowMapSize:{}}},spotLightMap:{value:[]},spotLightMatrix:{value:[]},pointLights:{value:[],properties:{color:{},position:{},decay:{},distance:{}}},pointLightShadows:{value:[],properties:{shadowIntensity:1,shadowBias:{},shadowNormalBias:{},shadowRadius:{},shadowMapSize:{},shadowCameraNear:{},shadowCameraFar:{}}},pointShadowMatrix:{value:[]},hemisphereLights:{value:[],properties:{direction:{},skyColor:{},groundColor:{}}},rectAreaLights:{value:[],properties:{color:{},position:{},width:{},height:{}}},ltc_1:{value:null},ltc_2:{value:null}},points:{diffuse:{value:new Xe(16777215)},opacity:{value:1},size:{value:1},scale:{value:1},map:{value:null},alphaMap:{value:null},alphaMapTransform:{value:new Be},alphaTest:{value:0},uvTransform:{value:new Be}},sprite:{diffuse:{value:new Xe(16777215)},opacity:{value:1},center:{value:new ht(.5,.5)},rotation:{value:0},map:{value:null},mapTransform:{value:new Be},alphaMap:{value:null},alphaMapTransform:{value:new Be},alphaTest:{value:0}}},Ni={basic:{uniforms:pn([oe.common,oe.specularmap,oe.envmap,oe.aomap,oe.lightmap,oe.fog]),vertexShader:ze.meshbasic_vert,fragmentShader:ze.meshbasic_frag},lambert:{uniforms:pn([oe.common,oe.specularmap,oe.envmap,oe.aomap,oe.lightmap,oe.emissivemap,oe.bumpmap,oe.normalmap,oe.displacementmap,oe.fog,oe.lights,{emissive:{value:new Xe(0)},envMapIntensity:{value:1}}]),vertexShader:ze.meshlambert_vert,fragmentShader:ze.meshlambert_frag},phong:{uniforms:pn([oe.common,oe.specularmap,oe.envmap,oe.aomap,oe.lightmap,oe.emissivemap,oe.bumpmap,oe.normalmap,oe.displacementmap,oe.fog,oe.lights,{emissive:{value:new Xe(0)},specular:{value:new Xe(1118481)},shininess:{value:30},envMapIntensity:{value:1}}]),vertexShader:ze.meshphong_vert,fragmentShader:ze.meshphong_frag},standard:{uniforms:pn([oe.common,oe.envmap,oe.aomap,oe.lightmap,oe.emissivemap,oe.bumpmap,oe.normalmap,oe.displacementmap,oe.roughnessmap,oe.metalnessmap,oe.fog,oe.lights,{emissive:{value:new Xe(0)},roughness:{value:1},metalness:{value:0},envMapIntensity:{value:1}}]),vertexShader:ze.meshphysical_vert,fragmentShader:ze.meshphysical_frag},toon:{uniforms:pn([oe.common,oe.aomap,oe.lightmap,oe.emissivemap,oe.bumpmap,oe.normalmap,oe.displacementmap,oe.gradientmap,oe.fog,oe.lights,{emissive:{value:new Xe(0)}}]),vertexShader:ze.meshtoon_vert,fragmentShader:ze.meshtoon_frag},matcap:{uniforms:pn([oe.common,oe.bumpmap,oe.normalmap,oe.displacementmap,oe.fog,{matcap:{value:null}}]),vertexShader:ze.meshmatcap_vert,fragmentShader:ze.meshmatcap_frag},points:{uniforms:pn([oe.points,oe.fog]),vertexShader:ze.points_vert,fragmentShader:ze.points_frag},dashed:{uniforms:pn([oe.common,oe.fog,{scale:{value:1},dashSize:{value:1},totalSize:{value:2}}]),vertexShader:ze.linedashed_vert,fragmentShader:ze.linedashed_frag},depth:{uniforms:pn([oe.common,oe.displacementmap]),vertexShader:ze.depth_vert,fragmentShader:ze.depth_frag},normal:{uniforms:pn([oe.common,oe.bumpmap,oe.normalmap,oe.displacementmap,{opacity:{value:1}}]),vertexShader:ze.meshnormal_vert,fragmentShader:ze.meshnormal_frag},sprite:{uniforms:pn([oe.sprite,oe.fog]),vertexShader:ze.sprite_vert,fragmentShader:ze.sprite_frag},background:{uniforms:{uvTransform:{value:new Be},t2D:{value:null},backgroundIntensity:{value:1}},vertexShader:ze.background_vert,fragmentShader:ze.background_frag},backgroundCube:{uniforms:{envMap:{value:null},flipEnvMap:{value:-1},backgroundBlurriness:{value:0},backgroundIntensity:{value:1},backgroundRotation:{value:new Be}},vertexShader:ze.backgroundCube_vert,fragmentShader:ze.backgroundCube_frag},cube:{uniforms:{tCube:{value:null},tFlip:{value:-1},opacity:{value:1}},vertexShader:ze.cube_vert,fragmentShader:ze.cube_frag},equirect:{uniforms:{tEquirect:{value:null}},vertexShader:ze.equirect_vert,fragmentShader:ze.equirect_frag},distance:{uniforms:pn([oe.common,oe.displacementmap,{referencePosition:{value:new k},nearDistance:{value:1},farDistance:{value:1e3}}]),vertexShader:ze.distance_vert,fragmentShader:ze.distance_frag},shadow:{uniforms:pn([oe.lights,oe.fog,{color:{value:new Xe(0)},opacity:{value:1}}]),vertexShader:ze.shadow_vert,fragmentShader:ze.shadow_frag}};Ni.physical={uniforms:pn([Ni.standard.uniforms,{clearcoat:{value:0},clearcoatMap:{value:null},clearcoatMapTransform:{value:new Be},clearcoatNormalMap:{value:null},clearcoatNormalMapTransform:{value:new Be},clearcoatNormalScale:{value:new ht(1,1)},clearcoatRoughness:{value:0},clearcoatRoughnessMap:{value:null},clearcoatRoughnessMapTransform:{value:new Be},dispersion:{value:0},iridescence:{value:0},iridescenceMap:{value:null},iridescenceMapTransform:{value:new Be},iridescenceIOR:{value:1.3},iridescenceThicknessMinimum:{value:100},iridescenceThicknessMaximum:{value:400},iridescenceThicknessMap:{value:null},iridescenceThicknessMapTransform:{value:new Be},sheen:{value:0},sheenColor:{value:new Xe(0)},sheenColorMap:{value:null},sheenColorMapTransform:{value:new Be},sheenRoughness:{value:1},sheenRoughnessMap:{value:null},sheenRoughnessMapTransform:{value:new Be},transmission:{value:0},transmissionMap:{value:null},transmissionMapTransform:{value:new Be},transmissionSamplerSize:{value:new ht},transmissionSamplerMap:{value:null},thickness:{value:0},thicknessMap:{value:null},thicknessMapTransform:{value:new Be},attenuationDistance:{value:0},attenuationColor:{value:new Xe(0)},specularColor:{value:new Xe(1,1,1)},specularColorMap:{value:null},specularColorMapTransform:{value:new Be},specularIntensity:{value:1},specularIntensityMap:{value:null},specularIntensityMapTransform:{value:new Be},anisotropyVector:{value:new ht},anisotropyMap:{value:null},anisotropyMapTransform:{value:new Be}}]),vertexShader:ze.meshphysical_vert,fragmentShader:ze.meshphysical_frag};var Od={r:0,b:0,g:0},Cs=new bs,SD=new Ut;function ED(n,e,t,i,r,s){let o=new Xe(0),a=r===!0?0:1,c,l,u=null,f=0,d=null;function h(M){let T=M.isScene===!0?M.background:null;if(T&&T.isTexture){let E=M.backgroundBlurriness>0;T=e.get(T,E)}return T}function g(M){let T=!1,E=h(M);E===null?m(o,a):E&&E.isColor&&(m(E,1),T=!0);let A=n.xr.getEnvironmentBlendMode();A==="additive"?t.buffers.color.setClear(0,0,0,1,s):A==="alpha-blend"&&t.buffers.color.setClear(0,0,0,0,s),(n.autoClear||T)&&(t.buffers.depth.setTest(!0),t.buffers.depth.setMask(!0),t.buffers.color.setMask(!0),n.clear(n.autoClearColor,n.autoClearDepth,n.autoClearStencil))}function x(M,T){let E=h(T);E&&(E.isCubeTexture||E.mapping===Za)?(l===void 0&&(l=new Xn(new wo(1,1,1),new Pn({name:"BackgroundCubeMaterial",uniforms:ws(Ni.backgroundCube.uniforms),vertexShader:Ni.backgroundCube.vertexShader,fragmentShader:Ni.backgroundCube.fragmentShader,side:vn,depthTest:!1,depthWrite:!1,fog:!1,allowOverride:!1})),l.geometry.deleteAttribute("normal"),l.geometry.deleteAttribute("uv"),l.onBeforeRender=function(A,I,D){this.matrixWorld.copyPosition(D.matrixWorld)},Object.defineProperty(l.material,"envMap",{get:function(){return this.uniforms.envMap.value}}),i.update(l)),Cs.copy(T.backgroundRotation),Cs.x*=-1,Cs.y*=-1,Cs.z*=-1,E.isCubeTexture&&E.isRenderTargetTexture===!1&&(Cs.y*=-1,Cs.z*=-1),l.material.uniforms.envMap.value=E,l.material.uniforms.flipEnvMap.value=E.isCubeTexture&&E.isRenderTargetTexture===!1?-1:1,l.material.uniforms.backgroundBlurriness.value=T.backgroundBlurriness,l.material.uniforms.backgroundIntensity.value=T.backgroundIntensity,l.material.uniforms.backgroundRotation.value.setFromMatrix4(SD.makeRotationFromEuler(Cs)),l.material.toneMapped=tt.getTransfer(E.colorSpace)!==ut,(u!==E||f!==E.version||d!==n.toneMapping)&&(l.material.needsUpdate=!0,u=E,f=E.version,d=n.toneMapping),l.layers.enableAll(),M.unshift(l,l.geometry,l.material,0,0,null)):E&&E.isTexture&&(c===void 0&&(c=new Xn(new Wa(2,2),new Pn({name:"BackgroundMaterial",uniforms:ws(Ni.background.uniforms),vertexShader:Ni.background.vertexShader,fragmentShader:Ni.background.fragmentShader,side:er,depthTest:!1,depthWrite:!1,fog:!1,allowOverride:!1})),c.geometry.deleteAttribute("normal"),Object.defineProperty(c.material,"map",{get:function(){return this.uniforms.t2D.value}}),i.update(c)),c.material.uniforms.t2D.value=E,c.material.uniforms.backgroundIntensity.value=T.backgroundIntensity,c.material.toneMapped=tt.getTransfer(E.colorSpace)!==ut,E.matrixAutoUpdate===!0&&E.updateMatrix(),c.material.uniforms.uvTransform.value.copy(E.matrix),(u!==E||f!==E.version||d!==n.toneMapping)&&(c.material.needsUpdate=!0,u=E,f=E.version,d=n.toneMapping),c.layers.enableAll(),M.unshift(c,c.geometry,c.material,0,0,null))}function m(M,T){M.getRGB(Od,ag(n)),t.buffers.color.setClear(Od.r,Od.g,Od.b,T,s)}function p(){l!==void 0&&(l.geometry.dispose(),l.material.dispose(),l=void 0),c!==void 0&&(c.geometry.dispose(),c.material.dispose(),c=void 0)}return{getClearColor:function(){return o},setClearColor:function(M,T=1){o.set(M),a=T,m(o,a)},getClearAlpha:function(){return a},setClearAlpha:function(M){a=M,m(o,a)},render:g,addToRenderList:x,dispose:p}}function TD(n,e){let t=n.getParameter(n.MAX_VERTEX_ATTRIBS),i={},r=d(null),s=r,o=!1;function a(w,U,V,W,B){let H=!1,L=f(w,W,V,U);s!==L&&(s=L,l(s.object)),H=h(w,W,V,B),H&&g(w,W,V,B),B!==null&&e.update(B,n.ELEMENT_ARRAY_BUFFER),(H||o)&&(o=!1,E(w,U,V,W),B!==null&&n.bindBuffer(n.ELEMENT_ARRAY_BUFFER,e.get(B).buffer))}function c(){return n.createVertexArray()}function l(w){return n.bindVertexArray(w)}function u(w){return n.deleteVertexArray(w)}function f(w,U,V,W){let B=W.wireframe===!0,H=i[U.id];H===void 0&&(H={},i[U.id]=H);let L=w.isInstancedMesh===!0?w.id:0,Q=H[L];Q===void 0&&(Q={},H[L]=Q);let Z=Q[V.id];Z===void 0&&(Z={},Q[V.id]=Z);let ue=Z[B];return ue===void 0&&(ue=d(c()),Z[B]=ue),ue}function d(w){let U=[],V=[],W=[];for(let B=0;B<t;B++)U[B]=0,V[B]=0,W[B]=0;return{geometry:null,program:null,wireframe:!1,newAttributes:U,enabledAttributes:V,attributeDivisors:W,object:w,attributes:{},index:null}}function h(w,U,V,W){let B=s.attributes,H=U.attributes,L=0,Q=V.getAttributes();for(let Z in Q)if(Q[Z].location>=0){let ge=B[Z],fe=H[Z];if(fe===void 0&&(Z==="instanceMatrix"&&w.instanceMatrix&&(fe=w.instanceMatrix),Z==="instanceColor"&&w.instanceColor&&(fe=w.instanceColor)),ge===void 0||ge.attribute!==fe||fe&&ge.data!==fe.data)return!0;L++}return s.attributesNum!==L||s.index!==W}function g(w,U,V,W){let B={},H=U.attributes,L=0,Q=V.getAttributes();for(let Z in Q)if(Q[Z].location>=0){let ge=H[Z];ge===void 0&&(Z==="instanceMatrix"&&w.instanceMatrix&&(ge=w.instanceMatrix),Z==="instanceColor"&&w.instanceColor&&(ge=w.instanceColor));let fe={};fe.attribute=ge,ge&&ge.data&&(fe.data=ge.data),B[Z]=fe,L++}s.attributes=B,s.attributesNum=L,s.index=W}function x(){let w=s.newAttributes;for(let U=0,V=w.length;U<V;U++)w[U]=0}function m(w){p(w,0)}function p(w,U){let V=s.newAttributes,W=s.enabledAttributes,B=s.attributeDivisors;V[w]=1,W[w]===0&&(n.enableVertexAttribArray(w),W[w]=1),B[w]!==U&&(n.vertexAttribDivisor(w,U),B[w]=U)}function M(){let w=s.newAttributes,U=s.enabledAttributes;for(let V=0,W=U.length;V<W;V++)U[V]!==w[V]&&(n.disableVertexAttribArray(V),U[V]=0)}function T(w,U,V,W,B,H,L){L===!0?n.vertexAttribIPointer(w,U,V,B,H):n.vertexAttribPointer(w,U,V,W,B,H)}function E(w,U,V,W){x();let B=W.attributes,H=V.getAttributes(),L=U.defaultAttributeValues;for(let Q in H){let Z=H[Q];if(Z.location>=0){let ue=B[Q];if(ue===void 0&&(Q==="instanceMatrix"&&w.instanceMatrix&&(ue=w.instanceMatrix),Q==="instanceColor"&&w.instanceColor&&(ue=w.instanceColor)),ue!==void 0){let ge=ue.normalized,fe=ue.itemSize,He=e.get(ue);if(He===void 0)continue;let Tt=He.buffer,St=He.type,X=He.bytesPerElement,ne=St===n.INT||St===n.UNSIGNED_INT||ue.gpuType===Yu;if(ue.isInterleavedBufferAttribute){let se=ue.data,Ve=se.stride,Ie=ue.offset;if(se.isInstancedInterleavedBuffer){for(let Pe=0;Pe<Z.locationSize;Pe++)p(Z.location+Pe,se.meshPerAttribute);w.isInstancedMesh!==!0&&W._maxInstanceCount===void 0&&(W._maxInstanceCount=se.meshPerAttribute*se.count)}else for(let Pe=0;Pe<Z.locationSize;Pe++)m(Z.location+Pe);n.bindBuffer(n.ARRAY_BUFFER,Tt);for(let Pe=0;Pe<Z.locationSize;Pe++)T(Z.location+Pe,fe/Z.locationSize,St,ge,Ve*X,(Ie+fe/Z.locationSize*Pe)*X,ne)}else{if(ue.isInstancedBufferAttribute){for(let se=0;se<Z.locationSize;se++)p(Z.location+se,ue.meshPerAttribute);w.isInstancedMesh!==!0&&W._maxInstanceCount===void 0&&(W._maxInstanceCount=ue.meshPerAttribute*ue.count)}else for(let se=0;se<Z.locationSize;se++)m(Z.location+se);n.bindBuffer(n.ARRAY_BUFFER,Tt);for(let se=0;se<Z.locationSize;se++)T(Z.location+se,fe/Z.locationSize,St,ge,fe*X,fe/Z.locationSize*se*X,ne)}}else if(L!==void 0){let ge=L[Q];if(ge!==void 0)switch(ge.length){case 2:n.vertexAttrib2fv(Z.location,ge);break;case 3:n.vertexAttrib3fv(Z.location,ge);break;case 4:n.vertexAttrib4fv(Z.location,ge);break;default:n.vertexAttrib1fv(Z.location,ge)}}}}M()}function A(){S();for(let w in i){let U=i[w];for(let V in U){let W=U[V];for(let B in W){let H=W[B];for(let L in H)u(H[L].object),delete H[L];delete W[B]}}delete i[w]}}function I(w){if(i[w.id]===void 0)return;let U=i[w.id];for(let V in U){let W=U[V];for(let B in W){let H=W[B];for(let L in H)u(H[L].object),delete H[L];delete W[B]}}delete i[w.id]}function D(w){for(let U in i){let V=i[U];for(let W in V){let B=V[W];if(B[w.id]===void 0)continue;let H=B[w.id];for(let L in H)u(H[L].object),delete H[L];delete B[w.id]}}}function y(w){for(let U in i){let V=i[U],W=w.isInstancedMesh===!0?w.id:0,B=V[W];if(B!==void 0){for(let H in B){let L=B[H];for(let Q in L)u(L[Q].object),delete L[Q];delete B[H]}delete V[W],Object.keys(V).length===0&&delete i[U]}}}function S(){q(),o=!0,s!==r&&(s=r,l(s.object))}function q(){r.geometry=null,r.program=null,r.wireframe=!1}return{setup:a,reset:S,resetDefaultState:q,dispose:A,releaseStatesOfGeometry:I,releaseStatesOfObject:y,releaseStatesOfProgram:D,initAttributes:x,enableAttribute:m,disableUnusedAttributes:M}}function wD(n,e,t){let i;function r(l){i=l}function s(l,u){n.drawArrays(i,l,u),t.update(u,i,1)}function o(l,u,f){f!==0&&(n.drawArraysInstanced(i,l,u,f),t.update(u,i,f))}function a(l,u,f){if(f===0)return;e.get("WEBGL_multi_draw").multiDrawArraysWEBGL(i,l,0,u,0,f);let h=0;for(let g=0;g<f;g++)h+=u[g];t.update(h,i,1)}function c(l,u,f,d){if(f===0)return;let h=e.get("WEBGL_multi_draw");if(h===null)for(let g=0;g<l.length;g++)o(l[g],u[g],d[g]);else{h.multiDrawArraysInstancedWEBGL(i,l,0,u,0,d,0,f);let g=0;for(let x=0;x<f;x++)g+=u[x]*d[x];t.update(g,i,1)}}this.setMode=r,this.render=s,this.renderInstances=o,this.renderMultiDraw=a,this.renderMultiDrawInstances=c}function CD(n,e,t,i){let r;function s(){if(r!==void 0)return r;if(e.has("EXT_texture_filter_anisotropic")===!0){let D=e.get("EXT_texture_filter_anisotropic");r=n.getParameter(D.MAX_TEXTURE_MAX_ANISOTROPY_EXT)}else r=0;return r}function o(D){return!(D!==Yn&&i.convert(D)!==n.getParameter(n.IMPLEMENTATION_COLOR_READ_FORMAT))}function a(D){let y=D===Di&&(e.has("EXT_color_buffer_half_float")||e.has("EXT_color_buffer_float"));return!(D!==On&&i.convert(D)!==n.getParameter(n.IMPLEMENTATION_COLOR_READ_TYPE)&&D!==hi&&!y)}function c(D){if(D==="highp"){if(n.getShaderPrecisionFormat(n.VERTEX_SHADER,n.HIGH_FLOAT).precision>0&&n.getShaderPrecisionFormat(n.FRAGMENT_SHADER,n.HIGH_FLOAT).precision>0)return"highp";D="mediump"}return D==="mediump"&&n.getShaderPrecisionFormat(n.VERTEX_SHADER,n.MEDIUM_FLOAT).precision>0&&n.getShaderPrecisionFormat(n.FRAGMENT_SHADER,n.MEDIUM_FLOAT).precision>0?"mediump":"lowp"}let l=t.precision!==void 0?t.precision:"highp",u=c(l);u!==l&&(Ne("WebGLRenderer:",l,"not supported, using",u,"instead."),l=u);let f=t.logarithmicDepthBuffer===!0,d=t.reversedDepthBuffer===!0&&e.has("EXT_clip_control"),h=n.getParameter(n.MAX_TEXTURE_IMAGE_UNITS),g=n.getParameter(n.MAX_VERTEX_TEXTURE_IMAGE_UNITS),x=n.getParameter(n.MAX_TEXTURE_SIZE),m=n.getParameter(n.MAX_CUBE_MAP_TEXTURE_SIZE),p=n.getParameter(n.MAX_VERTEX_ATTRIBS),M=n.getParameter(n.MAX_VERTEX_UNIFORM_VECTORS),T=n.getParameter(n.MAX_VARYING_VECTORS),E=n.getParameter(n.MAX_FRAGMENT_UNIFORM_VECTORS),A=n.getParameter(n.MAX_SAMPLES),I=n.getParameter(n.SAMPLES);return{isWebGL2:!0,getMaxAnisotropy:s,getMaxPrecision:c,textureFormatReadable:o,textureTypeReadable:a,precision:l,logarithmicDepthBuffer:f,reversedDepthBuffer:d,maxTextures:h,maxVertexTextures:g,maxTextureSize:x,maxCubemapSize:m,maxAttributes:p,maxVertexUniforms:M,maxVaryings:T,maxFragmentUniforms:E,maxSamples:A,samples:I}}function ID(n){let e=this,t=null,i=0,r=!1,s=!1,o=new Si,a=new Be,c={value:null,needsUpdate:!1};this.uniform=c,this.numPlanes=0,this.numIntersection=0,this.init=function(f,d){let h=f.length!==0||d||i!==0||r;return r=d,i=f.length,h},this.beginShadows=function(){s=!0,u(null)},this.endShadows=function(){s=!1},this.setGlobalState=function(f,d){t=u(f,d,0)},this.setState=function(f,d,h){let g=f.clippingPlanes,x=f.clipIntersection,m=f.clipShadows,p=n.get(f);if(!r||g===null||g.length===0||s&&!m)s?u(null):l();else{let M=s?0:i,T=M*4,E=p.clippingState||null;c.value=E,E=u(g,d,T,h);for(let A=0;A!==T;++A)E[A]=t[A];p.clippingState=E,this.numIntersection=x?this.numPlanes:0,this.numPlanes+=M}};function l(){c.value!==t&&(c.value=t,c.needsUpdate=i>0),e.numPlanes=i,e.numIntersection=0}function u(f,d,h,g){let x=f!==null?f.length:0,m=null;if(x!==0){if(m=c.value,g!==!0||m===null){let p=h+x*4,M=d.matrixWorldInverse;a.getNormalMatrix(M),(m===null||m.length<p)&&(m=new Float32Array(p));for(let T=0,E=h;T!==x;++T,E+=4)o.copy(f[T]).applyMatrix4(M,a),o.normal.toArray(m,E),m[E+3]=o.constant}c.value=m,c.needsUpdate=!0}return e.numPlanes=x,e.numIntersection=0,m}}var kr=4,j0=[.125,.215,.35,.446,.526,.582],As=20,AD=256,ic=new qa,$0=new Xe,dg=null,fg=0,hg=0,pg=!1,DD=new k,kd=class{constructor(e){this._renderer=e,this._pingPongRenderTarget=null,this._lodMax=0,this._cubeSize=0,this._sizeLods=[],this._sigmas=[],this._lodMeshes=[],this._backgroundBox=null,this._cubemapMaterial=null,this._equirectMaterial=null,this._blurMaterial=null,this._ggxMaterial=null}fromScene(e,t=0,i=.1,r=100,s={}){let{size:o=256,position:a=DD}=s;dg=this._renderer.getRenderTarget(),fg=this._renderer.getActiveCubeFace(),hg=this._renderer.getActiveMipmapLevel(),pg=this._renderer.xr.enabled,this._renderer.xr.enabled=!1,this._setSize(o);let c=this._allocateTargets();return c.depthBuffer=!0,this._sceneToCubeUV(e,i,r,c,a),t>0&&this._blur(c,0,0,t),this._applyPMREM(c),this._cleanup(c),c}fromEquirectangular(e,t=null){return this._fromTexture(e,t)}fromCubemap(e,t=null){return this._fromTexture(e,t)}compileCubemapShader(){this._cubemapMaterial===null&&(this._cubemapMaterial=Y0(),this._compileMaterial(this._cubemapMaterial))}compileEquirectangularShader(){this._equirectMaterial===null&&(this._equirectMaterial=X0(),this._compileMaterial(this._equirectMaterial))}dispose(){this._dispose(),this._cubemapMaterial!==null&&this._cubemapMaterial.dispose(),this._equirectMaterial!==null&&this._equirectMaterial.dispose(),this._backgroundBox!==null&&(this._backgroundBox.geometry.dispose(),this._backgroundBox.material.dispose())}_setSize(e){this._lodMax=Math.floor(Math.log2(e)),this._cubeSize=Math.pow(2,this._lodMax)}_dispose(){this._blurMaterial!==null&&this._blurMaterial.dispose(),this._ggxMaterial!==null&&this._ggxMaterial.dispose(),this._pingPongRenderTarget!==null&&this._pingPongRenderTarget.dispose();for(let e=0;e<this._lodMeshes.length;e++)this._lodMeshes[e].geometry.dispose()}_cleanup(e){this._renderer.setRenderTarget(dg,fg,hg),this._renderer.xr.enabled=pg,e.scissorTest=!1,Do(e,0,0,e.width,e.height)}_fromTexture(e,t){e.mapping===Lr||e.mapping===Es?this._setSize(e.image.length===0?16:e.image[0].width||e.image[0].image.width):this._setSize(e.image.width/4),dg=this._renderer.getRenderTarget(),fg=this._renderer.getActiveCubeFace(),hg=this._renderer.getActiveMipmapLevel(),pg=this._renderer.xr.enabled,this._renderer.xr.enabled=!1;let i=t||this._allocateTargets();return this._textureToCubeUV(e,i),this._applyPMREM(i),this._cleanup(i),i}_allocateTargets(){let e=3*Math.max(this._cubeSize,112),t=4*this._cubeSize,i={magFilter:tn,minFilter:tn,generateMipmaps:!1,type:Di,format:Yn,colorSpace:Ms,depthBuffer:!1},r=q0(e,t,i);if(this._pingPongRenderTarget===null||this._pingPongRenderTarget.width!==e||this._pingPongRenderTarget.height!==t){this._pingPongRenderTarget!==null&&this._dispose(),this._pingPongRenderTarget=q0(e,t,i);let{_lodMax:s}=this;({lodMeshes:this._lodMeshes,sizeLods:this._sizeLods,sigmas:this._sigmas}=RD(s)),this._blurMaterial=PD(s,e,t),this._ggxMaterial=ND(s,e,t)}return r}_compileMaterial(e){let t=new Xn(new Nn,e);this._renderer.compile(t,ic)}_sceneToCubeUV(e,t,i,r,s){let c=new fn(90,1,t,i),l=[1,-1,1,1,1,1],u=[1,1,1,-1,-1,-1],f=this._renderer,d=f.autoClear,h=f.toneMapping;f.getClearColor($0),f.toneMapping=di,f.autoClear=!1,f.state.buffers.depth.getReversed()&&(f.setRenderTarget(r),f.clearDepth(),f.setRenderTarget(null)),this._backgroundBox===null&&(this._backgroundBox=new Xn(new wo,new Ba({name:"PMREM.Background",side:vn,depthWrite:!1,depthTest:!1})));let x=this._backgroundBox,m=x.material,p=!1,M=e.background;M?M.isColor&&(m.color.copy(M),e.background=null,p=!0):(m.color.copy($0),p=!0);for(let T=0;T<6;T++){let E=T%3;E===0?(c.up.set(0,l[T],0),c.position.set(s.x,s.y,s.z),c.lookAt(s.x+u[T],s.y,s.z)):E===1?(c.up.set(0,0,l[T]),c.position.set(s.x,s.y,s.z),c.lookAt(s.x,s.y+u[T],s.z)):(c.up.set(0,l[T],0),c.position.set(s.x,s.y,s.z),c.lookAt(s.x,s.y,s.z+u[T]));let A=this._cubeSize;Do(r,E*A,T>2?A:0,A,A),f.setRenderTarget(r),p&&f.render(x,c),f.render(e,c)}f.toneMapping=h,f.autoClear=d,e.background=M}_textureToCubeUV(e,t){let i=this._renderer,r=e.mapping===Lr||e.mapping===Es;r?(this._cubemapMaterial===null&&(this._cubemapMaterial=Y0()),this._cubemapMaterial.uniforms.flipEnvMap.value=e.isRenderTargetTexture===!1?-1:1):this._equirectMaterial===null&&(this._equirectMaterial=X0());let s=r?this._cubemapMaterial:this._equirectMaterial,o=this._lodMeshes[0];o.material=s;let a=s.uniforms;a.envMap.value=e;let c=this._cubeSize;Do(t,0,0,3*c,2*c),i.setRenderTarget(t),i.render(o,ic)}_applyPMREM(e){let t=this._renderer,i=t.autoClear;t.autoClear=!1;let r=this._lodMeshes.length;for(let s=1;s<r;s++)this._applyGGXFilter(e,s-1,s);t.autoClear=i}_applyGGXFilter(e,t,i){let r=this._renderer,s=this._pingPongRenderTarget,o=this._ggxMaterial,a=this._lodMeshes[i];a.material=o;let c=o.uniforms,l=i/(this._lodMeshes.length-1),u=t/(this._lodMeshes.length-1),f=Math.sqrt(l*l-u*u),d=0+l*1.25,h=f*d,{_lodMax:g}=this,x=this._sizeLods[i],m=3*x*(i>g-kr?i-g+kr:0),p=4*(this._cubeSize-x);c.envMap.value=e.texture,c.roughness.value=h,c.mipInt.value=g-t,Do(s,m,p,3*x,2*x),r.setRenderTarget(s),r.render(a,ic),c.envMap.value=s.texture,c.roughness.value=0,c.mipInt.value=g-i,Do(e,m,p,3*x,2*x),r.setRenderTarget(e),r.render(a,ic)}_blur(e,t,i,r,s){let o=this._pingPongRenderTarget;this._halfBlur(e,o,t,i,r,"latitudinal",s),this._halfBlur(o,e,i,i,r,"longitudinal",s)}_halfBlur(e,t,i,r,s,o,a){let c=this._renderer,l=this._blurMaterial;o!=="latitudinal"&&o!=="longitudinal"&&Re("blur direction must be either latitudinal or longitudinal!");let u=3,f=this._lodMeshes[r];f.material=l;let d=l.uniforms,h=this._sizeLods[i]-1,g=isFinite(s)?Math.PI/(2*h):2*Math.PI/(2*As-1),x=s/g,m=isFinite(s)?1+Math.floor(u*x):As;m>As&&Ne(`sigmaRadians, ${s}, is too large and will clip, as it requested ${m} samples when the maximum is set to ${As}`);let p=[],M=0;for(let D=0;D<As;++D){let y=D/x,S=Math.exp(-y*y/2);p.push(S),D===0?M+=S:D<m&&(M+=2*S)}for(let D=0;D<p.length;D++)p[D]=p[D]/M;d.envMap.value=e.texture,d.samples.value=m,d.weights.value=p,d.latitudinal.value=o==="latitudinal",a&&(d.poleAxis.value=a);let{_lodMax:T}=this;d.dTheta.value=g,d.mipInt.value=T-i;let E=this._sizeLods[r],A=3*E*(r>T-kr?r-T+kr:0),I=4*(this._cubeSize-E);Do(t,A,I,3*E,2*E),c.setRenderTarget(t),c.render(f,ic)}};function RD(n){let e=[],t=[],i=[],r=n,s=n-kr+1+j0.length;for(let o=0;o<s;o++){let a=Math.pow(2,r);e.push(a);let c=1/a;o>n-kr?c=j0[o-n+kr-1]:o===0&&(c=0),t.push(c);let l=1/(a-2),u=-l,f=1+l,d=[u,u,f,u,f,f,u,u,f,f,u,f],h=6,g=6,x=3,m=2,p=1,M=new Float32Array(x*g*h),T=new Float32Array(m*g*h),E=new Float32Array(p*g*h);for(let I=0;I<h;I++){let D=I%3*2/3-1,y=I>2?0:-1,S=[D,y,0,D+2/3,y,0,D+2/3,y+1,0,D,y,0,D+2/3,y+1,0,D,y+1,0];M.set(S,x*g*I),T.set(d,m*g*I);let q=[I,I,I,I,I,I];E.set(q,p*g*I)}let A=new Nn;A.setAttribute("position",new hn(M,x)),A.setAttribute("uv",new hn(T,m)),A.setAttribute("faceIndex",new hn(E,p)),i.push(new Xn(A,null)),r>kr&&r--}return{lodMeshes:i,sizeLods:e,sigmas:t}}function q0(n,e,t){let i=new Rn(n,e,t);return i.texture.mapping=Za,i.texture.name="PMREM.cubeUv",i.scissorTest=!0,i}function Do(n,e,t,i,r){n.viewport.set(e,t,i,r),n.scissor.set(e,t,i,r)}function ND(n,e,t){return new Pn({name:"PMREMGGXConvolution",defines:{GGX_SAMPLES:AD,CUBEUV_TEXEL_WIDTH:1/e,CUBEUV_TEXEL_HEIGHT:1/t,CUBEUV_MAX_MIP:`${n}.0`},uniforms:{envMap:{value:null},roughness:{value:0},mipInt:{value:0}},vertexShader:Vd(),fragmentShader:`

			precision highp float;
			precision highp int;

			varying vec3 vOutputDirection;

			uniform sampler2D envMap;
			uniform float roughness;
			uniform float mipInt;

			#define ENVMAP_TYPE_CUBE_UV
			#include <cube_uv_reflection_fragment>

			#define PI 3.14159265359

			// Van der Corput radical inverse
			float radicalInverse_VdC(uint bits) {
				bits = (bits << 16u) | (bits >> 16u);
				bits = ((bits & 0x55555555u) << 1u) | ((bits & 0xAAAAAAAAu) >> 1u);
				bits = ((bits & 0x33333333u) << 2u) | ((bits & 0xCCCCCCCCu) >> 2u);
				bits = ((bits & 0x0F0F0F0Fu) << 4u) | ((bits & 0xF0F0F0F0u) >> 4u);
				bits = ((bits & 0x00FF00FFu) << 8u) | ((bits & 0xFF00FF00u) >> 8u);
				return float(bits) * 2.3283064365386963e-10; // / 0x100000000
			}

			// Hammersley sequence
			vec2 hammersley(uint i, uint N) {
				return vec2(float(i) / float(N), radicalInverse_VdC(i));
			}

			// GGX VNDF importance sampling (Eric Heitz 2018)
			// "Sampling the GGX Distribution of Visible Normals"
			// https://jcgt.org/published/0007/04/01/
			vec3 importanceSampleGGX_VNDF(vec2 Xi, vec3 V, float roughness) {
				float alpha = roughness * roughness;

				// Section 4.1: Orthonormal basis
				vec3 T1 = vec3(1.0, 0.0, 0.0);
				vec3 T2 = cross(V, T1);

				// Section 4.2: Parameterization of projected area
				float r = sqrt(Xi.x);
				float phi = 2.0 * PI * Xi.y;
				float t1 = r * cos(phi);
				float t2 = r * sin(phi);
				float s = 0.5 * (1.0 + V.z);
				t2 = (1.0 - s) * sqrt(1.0 - t1 * t1) + s * t2;

				// Section 4.3: Reprojection onto hemisphere
				vec3 Nh = t1 * T1 + t2 * T2 + sqrt(max(0.0, 1.0 - t1 * t1 - t2 * t2)) * V;

				// Section 3.4: Transform back to ellipsoid configuration
				return normalize(vec3(alpha * Nh.x, alpha * Nh.y, max(0.0, Nh.z)));
			}

			void main() {
				vec3 N = normalize(vOutputDirection);
				vec3 V = N; // Assume view direction equals normal for pre-filtering

				vec3 prefilteredColor = vec3(0.0);
				float totalWeight = 0.0;

				// For very low roughness, just sample the environment directly
				if (roughness < 0.001) {
					gl_FragColor = vec4(bilinearCubeUV(envMap, N, mipInt), 1.0);
					return;
				}

				// Tangent space basis for VNDF sampling
				vec3 up = abs(N.z) < 0.999 ? vec3(0.0, 0.0, 1.0) : vec3(1.0, 0.0, 0.0);
				vec3 tangent = normalize(cross(up, N));
				vec3 bitangent = cross(N, tangent);

				for(uint i = 0u; i < uint(GGX_SAMPLES); i++) {
					vec2 Xi = hammersley(i, uint(GGX_SAMPLES));

					// For PMREM, V = N, so in tangent space V is always (0, 0, 1)
					vec3 H_tangent = importanceSampleGGX_VNDF(Xi, vec3(0.0, 0.0, 1.0), roughness);

					// Transform H back to world space
					vec3 H = normalize(tangent * H_tangent.x + bitangent * H_tangent.y + N * H_tangent.z);
					vec3 L = normalize(2.0 * dot(V, H) * H - V);

					float NdotL = max(dot(N, L), 0.0);

					if(NdotL > 0.0) {
						// Sample environment at fixed mip level
						// VNDF importance sampling handles the distribution filtering
						vec3 sampleColor = bilinearCubeUV(envMap, L, mipInt);

						// Weight by NdotL for the split-sum approximation
						// VNDF PDF naturally accounts for the visible microfacet distribution
						prefilteredColor += sampleColor * NdotL;
						totalWeight += NdotL;
					}
				}

				if (totalWeight > 0.0) {
					prefilteredColor = prefilteredColor / totalWeight;
				}

				gl_FragColor = vec4(prefilteredColor, 1.0);
			}
		`,blending:Ai,depthTest:!1,depthWrite:!1})}function PD(n,e,t){let i=new Float32Array(As),r=new k(0,1,0);return new Pn({name:"SphericalGaussianBlur",defines:{n:As,CUBEUV_TEXEL_WIDTH:1/e,CUBEUV_TEXEL_HEIGHT:1/t,CUBEUV_MAX_MIP:`${n}.0`},uniforms:{envMap:{value:null},samples:{value:1},weights:{value:i},latitudinal:{value:!1},dTheta:{value:0},mipInt:{value:0},poleAxis:{value:r}},vertexShader:Vd(),fragmentShader:`

			precision mediump float;
			precision mediump int;

			varying vec3 vOutputDirection;

			uniform sampler2D envMap;
			uniform int samples;
			uniform float weights[ n ];
			uniform bool latitudinal;
			uniform float dTheta;
			uniform float mipInt;
			uniform vec3 poleAxis;

			#define ENVMAP_TYPE_CUBE_UV
			#include <cube_uv_reflection_fragment>

			vec3 getSample( float theta, vec3 axis ) {

				float cosTheta = cos( theta );
				// Rodrigues' axis-angle rotation
				vec3 sampleDirection = vOutputDirection * cosTheta
					+ cross( axis, vOutputDirection ) * sin( theta )
					+ axis * dot( axis, vOutputDirection ) * ( 1.0 - cosTheta );

				return bilinearCubeUV( envMap, sampleDirection, mipInt );

			}

			void main() {

				vec3 axis = latitudinal ? poleAxis : cross( poleAxis, vOutputDirection );

				if ( all( equal( axis, vec3( 0.0 ) ) ) ) {

					axis = vec3( vOutputDirection.z, 0.0, - vOutputDirection.x );

				}

				axis = normalize( axis );

				gl_FragColor = vec4( 0.0, 0.0, 0.0, 1.0 );
				gl_FragColor.rgb += weights[ 0 ] * getSample( 0.0, axis );

				for ( int i = 1; i < n; i++ ) {

					if ( i >= samples ) {

						break;

					}

					float theta = dTheta * float( i );
					gl_FragColor.rgb += weights[ i ] * getSample( -1.0 * theta, axis );
					gl_FragColor.rgb += weights[ i ] * getSample( theta, axis );

				}

			}
		`,blending:Ai,depthTest:!1,depthWrite:!1})}function X0(){return new Pn({name:"EquirectangularToCubeUV",uniforms:{envMap:{value:null}},vertexShader:Vd(),fragmentShader:`

			precision mediump float;
			precision mediump int;

			varying vec3 vOutputDirection;

			uniform sampler2D envMap;

			#include <common>

			void main() {

				vec3 outputDirection = normalize( vOutputDirection );
				vec2 uv = equirectUv( outputDirection );

				gl_FragColor = vec4( texture2D ( envMap, uv ).rgb, 1.0 );

			}
		`,blending:Ai,depthTest:!1,depthWrite:!1})}function Y0(){return new Pn({name:"CubemapToCubeUV",uniforms:{envMap:{value:null},flipEnvMap:{value:-1}},vertexShader:Vd(),fragmentShader:`

			precision mediump float;
			precision mediump int;

			uniform float flipEnvMap;

			varying vec3 vOutputDirection;

			uniform samplerCube envMap;

			void main() {

				gl_FragColor = textureCube( envMap, vec3( flipEnvMap * vOutputDirection.x, vOutputDirection.yz ) );

			}
		`,blending:Ai,depthTest:!1,depthWrite:!1})}function Vd(){return`

		precision mediump float;
		precision mediump int;

		attribute float faceIndex;

		varying vec3 vOutputDirection;

		// RH coordinate system; PMREM face-indexing convention
		vec3 getDirection( vec2 uv, float face ) {

			uv = 2.0 * uv - 1.0;

			vec3 direction = vec3( uv, 1.0 );

			if ( face == 0.0 ) {

				direction = direction.zyx; // ( 1, v, u ) pos x

			} else if ( face == 1.0 ) {

				direction = direction.xzy;
				direction.xz *= -1.0; // ( -u, 1, -v ) pos y

			} else if ( face == 2.0 ) {

				direction.x *= -1.0; // ( -u, v, 1 ) pos z

			} else if ( face == 3.0 ) {

				direction = direction.zyx;
				direction.xz *= -1.0; // ( -1, v, -u ) neg x

			} else if ( face == 4.0 ) {

				direction = direction.xzy;
				direction.xy *= -1.0; // ( -u, -1, v ) neg y

			} else if ( face == 5.0 ) {

				direction.z *= -1.0; // ( u, v, -1 ) neg z

			}

			return direction;

		}

		void main() {

			vOutputDirection = getDirection( uv, faceIndex );
			gl_Position = vec4( position, 1.0 );

		}
	`}var Ud=class extends Rn{constructor(e=1,t={}){super(e,e,t),this.isWebGLCubeRenderTarget=!0;let i={width:e,height:e,depth:1},r=[i,i,i,i,i,i];this.texture=new za(r),this._setTextureOptions(t),this.texture.isRenderTargetTexture=!0}fromEquirectangularTexture(e,t){this.texture.type=t.type,this.texture.colorSpace=t.colorSpace,this.texture.generateMipmaps=t.generateMipmaps,this.texture.minFilter=t.minFilter,this.texture.magFilter=t.magFilter;let i={uniforms:{tEquirect:{value:null}},vertexShader:`

				varying vec3 vWorldDirection;

				vec3 transformDirection( in vec3 dir, in mat4 matrix ) {

					return normalize( ( matrix * vec4( dir, 0.0 ) ).xyz );

				}

				void main() {

					vWorldDirection = transformDirection( position, modelMatrix );

					#include <begin_vertex>
					#include <project_vertex>

				}
			`,fragmentShader:`

				uniform sampler2D tEquirect;

				varying vec3 vWorldDirection;

				#include <common>

				void main() {

					vec3 direction = normalize( vWorldDirection );

					vec2 sampleUV = equirectUv( direction );

					gl_FragColor = texture2D( tEquirect, sampleUV );

				}
			`},r=new wo(5,5,5),s=new Pn({name:"CubemapFromEquirect",uniforms:ws(i.uniforms),vertexShader:i.vertexShader,fragmentShader:i.fragmentShader,side:vn,blending:Ai});s.uniforms.tEquirect.value=t;let o=new Xn(r,s),a=t.minFilter;return t.minFilter===Or&&(t.minFilter=tn),new Wu(1,10,this).update(e,o),t.minFilter=a,o.geometry.dispose(),o.material.dispose(),this}clear(e,t=!0,i=!0,r=!0){let s=e.getRenderTarget();for(let o=0;o<6;o++)e.setRenderTarget(this,o),e.clear(t,i,r);e.setRenderTarget(s)}};function LD(n){let e=new WeakMap,t=new WeakMap,i=null;function r(d,h=!1){return d==null?null:h?o(d):s(d)}function s(d){if(d&&d.isTexture){let h=d.mapping;if(h===$u||h===qu)if(e.has(d)){let g=e.get(d).texture;return a(g,d.mapping)}else{let g=d.image;if(g&&g.height>0){let x=new Ud(g.height);return x.fromEquirectangularTexture(n,d),e.set(d,x),d.addEventListener("dispose",l),a(x.texture,d.mapping)}else return null}}return d}function o(d){if(d&&d.isTexture){let h=d.mapping,g=h===$u||h===qu,x=h===Lr||h===Es;if(g||x){let m=t.get(d),p=m!==void 0?m.texture.pmremVersion:0;if(d.isRenderTargetTexture&&d.pmremVersion!==p)return i===null&&(i=new kd(n)),m=g?i.fromEquirectangular(d,m):i.fromCubemap(d,m),m.texture.pmremVersion=d.pmremVersion,t.set(d,m),m.texture;if(m!==void 0)return m.texture;{let M=d.image;return g&&M&&M.height>0||x&&M&&c(M)?(i===null&&(i=new kd(n)),m=g?i.fromEquirectangular(d):i.fromCubemap(d),m.texture.pmremVersion=d.pmremVersion,t.set(d,m),d.addEventListener("dispose",u),m.texture):null}}}return d}function a(d,h){return h===$u?d.mapping=Lr:h===qu&&(d.mapping=Es),d}function c(d){let h=0,g=6;for(let x=0;x<g;x++)d[x]!==void 0&&h++;return h===g}function l(d){let h=d.target;h.removeEventListener("dispose",l);let g=e.get(h);g!==void 0&&(e.delete(h),g.dispose())}function u(d){let h=d.target;h.removeEventListener("dispose",u);let g=t.get(h);g!==void 0&&(t.delete(h),g.dispose())}function f(){e=new WeakMap,t=new WeakMap,i!==null&&(i.dispose(),i=null)}return{get:r,dispose:f}}function OD(n){let e={};function t(i){if(e[i]!==void 0)return e[i];let r=n.getExtension(i);return e[i]=r,r}return{has:function(i){return t(i)!==null},init:function(){t("EXT_color_buffer_float"),t("WEBGL_clip_cull_distance"),t("OES_texture_float_linear"),t("EXT_color_buffer_half_float"),t("WEBGL_multisampled_render_to_texture"),t("WEBGL_render_shared_exponent")},get:function(i){let r=t(i);return r===null&&Na("WebGLRenderer: "+i+" extension not supported."),r}}}function FD(n,e,t,i){let r={},s=new WeakMap;function o(f){let d=f.target;d.index!==null&&e.remove(d.index);for(let g in d.attributes)e.remove(d.attributes[g]);d.removeEventListener("dispose",o),delete r[d.id];let h=s.get(d);h&&(e.remove(h),s.delete(d)),i.releaseStatesOfGeometry(d),d.isInstancedBufferGeometry===!0&&delete d._maxInstanceCount,t.memory.geometries--}function a(f,d){return r[d.id]===!0||(d.addEventListener("dispose",o),r[d.id]=!0,t.memory.geometries++),d}function c(f){let d=f.attributes;for(let h in d)e.update(d[h],n.ARRAY_BUFFER)}function l(f){let d=[],h=f.index,g=f.attributes.position,x=0;if(g===void 0)return;if(h!==null){let M=h.array;x=h.version;for(let T=0,E=M.length;T<E;T+=3){let A=M[T+0],I=M[T+1],D=M[T+2];d.push(A,I,I,D,D,A)}}else{let M=g.array;x=g.version;for(let T=0,E=M.length/3-1;T<E;T+=3){let A=T+0,I=T+1,D=T+2;d.push(A,I,I,D,D,A)}}let m=new(g.count>=65535?ka:Fa)(d,1);m.version=x;let p=s.get(f);p&&e.remove(p),s.set(f,m)}function u(f){let d=s.get(f);if(d){let h=f.index;h!==null&&d.version<h.version&&l(f)}else l(f);return s.get(f)}return{get:a,update:c,getWireframeAttribute:u}}function kD(n,e,t){let i;function r(d){i=d}let s,o;function a(d){s=d.type,o=d.bytesPerElement}function c(d,h){n.drawElements(i,h,s,d*o),t.update(h,i,1)}function l(d,h,g){g!==0&&(n.drawElementsInstanced(i,h,s,d*o,g),t.update(h,i,g))}function u(d,h,g){if(g===0)return;e.get("WEBGL_multi_draw").multiDrawElementsWEBGL(i,h,0,s,d,0,g);let m=0;for(let p=0;p<g;p++)m+=h[p];t.update(m,i,1)}function f(d,h,g,x){if(g===0)return;let m=e.get("WEBGL_multi_draw");if(m===null)for(let p=0;p<d.length;p++)l(d[p]/o,h[p],x[p]);else{m.multiDrawElementsInstancedWEBGL(i,h,0,s,d,0,x,0,g);let p=0;for(let M=0;M<g;M++)p+=h[M]*x[M];t.update(p,i,1)}}this.setMode=r,this.setIndex=a,this.render=c,this.renderInstances=l,this.renderMultiDraw=u,this.renderMultiDrawInstances=f}function UD(n){let e={geometries:0,textures:0},t={frame:0,calls:0,triangles:0,points:0,lines:0};function i(s,o,a){switch(t.calls++,o){case n.TRIANGLES:t.triangles+=a*(s/3);break;case n.LINES:t.lines+=a*(s/2);break;case n.LINE_STRIP:t.lines+=a*(s-1);break;case n.LINE_LOOP:t.lines+=a*s;break;case n.POINTS:t.points+=a*s;break;default:Re("WebGLInfo: Unknown draw mode:",o);break}}function r(){t.calls=0,t.triangles=0,t.points=0,t.lines=0}return{memory:e,render:t,programs:null,autoReset:!0,reset:r,update:i}}function BD(n,e,t){let i=new WeakMap,r=new Pt;function s(o,a,c){let l=o.morphTargetInfluences,u=a.morphAttributes.position||a.morphAttributes.normal||a.morphAttributes.color,f=u!==void 0?u.length:0,d=i.get(a);if(d===void 0||d.count!==f){let q=function(){y.dispose(),i.delete(a),a.removeEventListener("dispose",q)};var h=q;d!==void 0&&d.texture.dispose();let g=a.morphAttributes.position!==void 0,x=a.morphAttributes.normal!==void 0,m=a.morphAttributes.color!==void 0,p=a.morphAttributes.position||[],M=a.morphAttributes.normal||[],T=a.morphAttributes.color||[],E=0;g===!0&&(E=1),x===!0&&(E=2),m===!0&&(E=3);let A=a.attributes.position.count*E,I=1;A>e.maxTextureSize&&(I=Math.ceil(A/e.maxTextureSize),A=e.maxTextureSize);let D=new Float32Array(A*I*4*f),y=new Pa(D,A,I,f);y.type=hi,y.needsUpdate=!0;let S=E*4;for(let w=0;w<f;w++){let U=p[w],V=M[w],W=T[w],B=A*I*4*w;for(let H=0;H<U.count;H++){let L=H*S;g===!0&&(r.fromBufferAttribute(U,H),D[B+L+0]=r.x,D[B+L+1]=r.y,D[B+L+2]=r.z,D[B+L+3]=0),x===!0&&(r.fromBufferAttribute(V,H),D[B+L+4]=r.x,D[B+L+5]=r.y,D[B+L+6]=r.z,D[B+L+7]=0),m===!0&&(r.fromBufferAttribute(W,H),D[B+L+8]=r.x,D[B+L+9]=r.y,D[B+L+10]=r.z,D[B+L+11]=W.itemSize===4?r.w:1)}}d={count:f,texture:y,size:new ht(A,I)},i.set(a,d),a.addEventListener("dispose",q)}if(o.isInstancedMesh===!0&&o.morphTexture!==null)c.getUniforms().setValue(n,"morphTexture",o.morphTexture,t);else{let g=0;for(let m=0;m<l.length;m++)g+=l[m];let x=a.morphTargetsRelative?1:1-g;c.getUniforms().setValue(n,"morphTargetBaseInfluence",x),c.getUniforms().setValue(n,"morphTargetInfluences",l)}c.getUniforms().setValue(n,"morphTargetsTexture",d.texture,t),c.getUniforms().setValue(n,"morphTargetsTextureSize",d.size)}return{update:s}}function VD(n,e,t,i,r){let s=new WeakMap;function o(l){let u=r.render.frame,f=l.geometry,d=e.get(l,f);if(s.get(d)!==u&&(e.update(d),s.set(d,u)),l.isInstancedMesh&&(l.hasEventListener("dispose",c)===!1&&l.addEventListener("dispose",c),s.get(l)!==u&&(t.update(l.instanceMatrix,n.ARRAY_BUFFER),l.instanceColor!==null&&t.update(l.instanceColor,n.ARRAY_BUFFER),s.set(l,u))),l.isSkinnedMesh){let h=l.skeleton;s.get(h)!==u&&(h.update(),s.set(h,u))}return d}function a(){s=new WeakMap}function c(l){let u=l.target;u.removeEventListener("dispose",c),i.releaseStatesOfObject(u),t.remove(u.instanceMatrix),u.instanceColor!==null&&t.remove(u.instanceColor)}return{update:o,dispose:a}}var HD={[jm]:"LINEAR_TONE_MAPPING",[$m]:"REINHARD_TONE_MAPPING",[qm]:"CINEON_TONE_MAPPING",[Xm]:"ACES_FILMIC_TONE_MAPPING",[Zm]:"AGX_TONE_MAPPING",[Jm]:"NEUTRAL_TONE_MAPPING",[Ym]:"CUSTOM_TONE_MAPPING"};function zD(n,e,t,i,r){let s=new Rn(e,t,{type:n,depthBuffer:i,stencilBuffer:r}),o=new Rn(e,t,{type:Di,depthBuffer:!1,stencilBuffer:!1}),a=new Nn;a.setAttribute("position",new qn([-1,3,0,-1,-1,0,3,-1,0],3)),a.setAttribute("uv",new qn([0,2,0,0,2,0],2));let c=new Nu({uniforms:{tDiffuse:{value:null}},vertexShader:`
			precision highp float;

			uniform mat4 modelViewMatrix;
			uniform mat4 projectionMatrix;

			attribute vec3 position;
			attribute vec2 uv;

			varying vec2 vUv;

			void main() {
				vUv = uv;
				gl_Position = projectionMatrix * modelViewMatrix * vec4( position, 1.0 );
			}`,fragmentShader:`
			precision highp float;

			uniform sampler2D tDiffuse;

			varying vec2 vUv;

			#include <tonemapping_pars_fragment>
			#include <colorspace_pars_fragment>

			void main() {
				gl_FragColor = texture2D( tDiffuse, vUv );

				#ifdef LINEAR_TONE_MAPPING
					gl_FragColor.rgb = LinearToneMapping( gl_FragColor.rgb );
				#elif defined( REINHARD_TONE_MAPPING )
					gl_FragColor.rgb = ReinhardToneMapping( gl_FragColor.rgb );
				#elif defined( CINEON_TONE_MAPPING )
					gl_FragColor.rgb = CineonToneMapping( gl_FragColor.rgb );
				#elif defined( ACES_FILMIC_TONE_MAPPING )
					gl_FragColor.rgb = ACESFilmicToneMapping( gl_FragColor.rgb );
				#elif defined( AGX_TONE_MAPPING )
					gl_FragColor.rgb = AgXToneMapping( gl_FragColor.rgb );
				#elif defined( NEUTRAL_TONE_MAPPING )
					gl_FragColor.rgb = NeutralToneMapping( gl_FragColor.rgb );
				#elif defined( CUSTOM_TONE_MAPPING )
					gl_FragColor.rgb = CustomToneMapping( gl_FragColor.rgb );
				#endif

				#ifdef SRGB_TRANSFER
					gl_FragColor = sRGBTransferOETF( gl_FragColor );
				#endif
			}`,depthTest:!1,depthWrite:!1}),l=new Xn(a,c),u=new qa(-1,1,1,-1,0,1),f=null,d=null,h=!1,g,x=null,m=[],p=!1;this.setSize=function(M,T){s.setSize(M,T),o.setSize(M,T);for(let E=0;E<m.length;E++){let A=m[E];A.setSize&&A.setSize(M,T)}},this.setEffects=function(M){m=M,p=m.length>0&&m[0].isRenderPass===!0;let T=s.width,E=s.height;for(let A=0;A<m.length;A++){let I=m[A];I.setSize&&I.setSize(T,E)}},this.begin=function(M,T){if(h||M.toneMapping===di&&m.length===0)return!1;if(x=T,T!==null){let E=T.width,A=T.height;(s.width!==E||s.height!==A)&&this.setSize(E,A)}return p===!1&&M.setRenderTarget(s),g=M.toneMapping,M.toneMapping=di,!0},this.hasRenderPass=function(){return p},this.end=function(M,T){M.toneMapping=g,h=!0;let E=s,A=o;for(let I=0;I<m.length;I++){let D=m[I];if(D.enabled!==!1&&(D.render(M,A,E,T),D.needsSwap!==!1)){let y=E;E=A,A=y}}if(f!==M.outputColorSpace||d!==M.toneMapping){f=M.outputColorSpace,d=M.toneMapping,c.defines={},tt.getTransfer(f)===ut&&(c.defines.SRGB_TRANSFER="");let I=HD[d];I&&(c.defines[I]=""),c.needsUpdate=!0}c.uniforms.tDiffuse.value=E.texture,M.setRenderTarget(x),M.render(l,u),x=null,h=!1},this.isCompositing=function(){return h},this.dispose=function(){s.dispose(),o.dispose(),a.dispose(),c.dispose()}}var pM=new rr,vg=new Dr(1,1),mM=new Pa,gM=new Au,vM=new za,Z0=[],J0=[],K0=new Float32Array(16),Q0=new Float32Array(9),eM=new Float32Array(4);function No(n,e,t){let i=n[0];if(i<=0||i>0)return n;let r=e*t,s=Z0[r];if(s===void 0&&(s=new Float32Array(r),Z0[r]=s),e!==0){i.toArray(s,0);for(let o=1,a=0;o!==e;++o)a+=t,n[o].toArray(s,a)}return s}function qt(n,e){if(n.length!==e.length)return!1;for(let t=0,i=n.length;t<i;t++)if(n[t]!==e[t])return!1;return!0}function Xt(n,e){for(let t=0,i=e.length;t<i;t++)n[t]=e[t]}function Hd(n,e){let t=J0[e];t===void 0&&(t=new Int32Array(e),J0[e]=t);for(let i=0;i!==e;++i)t[i]=n.allocateTextureUnit();return t}function GD(n,e){let t=this.cache;t[0]!==e&&(n.uniform1f(this.addr,e),t[0]=e)}function WD(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y)&&(n.uniform2f(this.addr,e.x,e.y),t[0]=e.x,t[1]=e.y);else{if(qt(t,e))return;n.uniform2fv(this.addr,e),Xt(t,e)}}function jD(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y||t[2]!==e.z)&&(n.uniform3f(this.addr,e.x,e.y,e.z),t[0]=e.x,t[1]=e.y,t[2]=e.z);else if(e.r!==void 0)(t[0]!==e.r||t[1]!==e.g||t[2]!==e.b)&&(n.uniform3f(this.addr,e.r,e.g,e.b),t[0]=e.r,t[1]=e.g,t[2]=e.b);else{if(qt(t,e))return;n.uniform3fv(this.addr,e),Xt(t,e)}}function $D(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y||t[2]!==e.z||t[3]!==e.w)&&(n.uniform4f(this.addr,e.x,e.y,e.z,e.w),t[0]=e.x,t[1]=e.y,t[2]=e.z,t[3]=e.w);else{if(qt(t,e))return;n.uniform4fv(this.addr,e),Xt(t,e)}}function qD(n,e){let t=this.cache,i=e.elements;if(i===void 0){if(qt(t,e))return;n.uniformMatrix2fv(this.addr,!1,e),Xt(t,e)}else{if(qt(t,i))return;eM.set(i),n.uniformMatrix2fv(this.addr,!1,eM),Xt(t,i)}}function XD(n,e){let t=this.cache,i=e.elements;if(i===void 0){if(qt(t,e))return;n.uniformMatrix3fv(this.addr,!1,e),Xt(t,e)}else{if(qt(t,i))return;Q0.set(i),n.uniformMatrix3fv(this.addr,!1,Q0),Xt(t,i)}}function YD(n,e){let t=this.cache,i=e.elements;if(i===void 0){if(qt(t,e))return;n.uniformMatrix4fv(this.addr,!1,e),Xt(t,e)}else{if(qt(t,i))return;K0.set(i),n.uniformMatrix4fv(this.addr,!1,K0),Xt(t,i)}}function ZD(n,e){let t=this.cache;t[0]!==e&&(n.uniform1i(this.addr,e),t[0]=e)}function JD(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y)&&(n.uniform2i(this.addr,e.x,e.y),t[0]=e.x,t[1]=e.y);else{if(qt(t,e))return;n.uniform2iv(this.addr,e),Xt(t,e)}}function KD(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y||t[2]!==e.z)&&(n.uniform3i(this.addr,e.x,e.y,e.z),t[0]=e.x,t[1]=e.y,t[2]=e.z);else{if(qt(t,e))return;n.uniform3iv(this.addr,e),Xt(t,e)}}function QD(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y||t[2]!==e.z||t[3]!==e.w)&&(n.uniform4i(this.addr,e.x,e.y,e.z,e.w),t[0]=e.x,t[1]=e.y,t[2]=e.z,t[3]=e.w);else{if(qt(t,e))return;n.uniform4iv(this.addr,e),Xt(t,e)}}function e1(n,e){let t=this.cache;t[0]!==e&&(n.uniform1ui(this.addr,e),t[0]=e)}function t1(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y)&&(n.uniform2ui(this.addr,e.x,e.y),t[0]=e.x,t[1]=e.y);else{if(qt(t,e))return;n.uniform2uiv(this.addr,e),Xt(t,e)}}function n1(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y||t[2]!==e.z)&&(n.uniform3ui(this.addr,e.x,e.y,e.z),t[0]=e.x,t[1]=e.y,t[2]=e.z);else{if(qt(t,e))return;n.uniform3uiv(this.addr,e),Xt(t,e)}}function i1(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y||t[2]!==e.z||t[3]!==e.w)&&(n.uniform4ui(this.addr,e.x,e.y,e.z,e.w),t[0]=e.x,t[1]=e.y,t[2]=e.z,t[3]=e.w);else{if(qt(t,e))return;n.uniform4uiv(this.addr,e),Xt(t,e)}}function r1(n,e,t){let i=this.cache,r=t.allocateTextureUnit();i[0]!==r&&(n.uniform1i(this.addr,r),i[0]=r);let s;this.type===n.SAMPLER_2D_SHADOW?(vg.compareFunction=t.isReversedDepthBuffer()?Ld:Pd,s=vg):s=pM,t.setTexture2D(e||s,r)}function s1(n,e,t){let i=this.cache,r=t.allocateTextureUnit();i[0]!==r&&(n.uniform1i(this.addr,r),i[0]=r),t.setTexture3D(e||gM,r)}function o1(n,e,t){let i=this.cache,r=t.allocateTextureUnit();i[0]!==r&&(n.uniform1i(this.addr,r),i[0]=r),t.setTextureCube(e||vM,r)}function a1(n,e,t){let i=this.cache,r=t.allocateTextureUnit();i[0]!==r&&(n.uniform1i(this.addr,r),i[0]=r),t.setTexture2DArray(e||mM,r)}function c1(n){switch(n){case 5126:return GD;case 35664:return WD;case 35665:return jD;case 35666:return $D;case 35674:return qD;case 35675:return XD;case 35676:return YD;case 5124:case 35670:return ZD;case 35667:case 35671:return JD;case 35668:case 35672:return KD;case 35669:case 35673:return QD;case 5125:return e1;case 36294:return t1;case 36295:return n1;case 36296:return i1;case 35678:case 36198:case 36298:case 36306:case 35682:return r1;case 35679:case 36299:case 36307:return s1;case 35680:case 36300:case 36308:case 36293:return o1;case 36289:case 36303:case 36311:case 36292:return a1}}function l1(n,e){n.uniform1fv(this.addr,e)}function u1(n,e){let t=No(e,this.size,2);n.uniform2fv(this.addr,t)}function d1(n,e){let t=No(e,this.size,3);n.uniform3fv(this.addr,t)}function f1(n,e){let t=No(e,this.size,4);n.uniform4fv(this.addr,t)}function h1(n,e){let t=No(e,this.size,4);n.uniformMatrix2fv(this.addr,!1,t)}function p1(n,e){let t=No(e,this.size,9);n.uniformMatrix3fv(this.addr,!1,t)}function m1(n,e){let t=No(e,this.size,16);n.uniformMatrix4fv(this.addr,!1,t)}function g1(n,e){n.uniform1iv(this.addr,e)}function v1(n,e){n.uniform2iv(this.addr,e)}function y1(n,e){n.uniform3iv(this.addr,e)}function _1(n,e){n.uniform4iv(this.addr,e)}function x1(n,e){n.uniform1uiv(this.addr,e)}function M1(n,e){n.uniform2uiv(this.addr,e)}function b1(n,e){n.uniform3uiv(this.addr,e)}function S1(n,e){n.uniform4uiv(this.addr,e)}function E1(n,e,t){let i=this.cache,r=e.length,s=Hd(t,r);qt(i,s)||(n.uniform1iv(this.addr,s),Xt(i,s));let o;this.type===n.SAMPLER_2D_SHADOW?o=vg:o=pM;for(let a=0;a!==r;++a)t.setTexture2D(e[a]||o,s[a])}function T1(n,e,t){let i=this.cache,r=e.length,s=Hd(t,r);qt(i,s)||(n.uniform1iv(this.addr,s),Xt(i,s));for(let o=0;o!==r;++o)t.setTexture3D(e[o]||gM,s[o])}function w1(n,e,t){let i=this.cache,r=e.length,s=Hd(t,r);qt(i,s)||(n.uniform1iv(this.addr,s),Xt(i,s));for(let o=0;o!==r;++o)t.setTextureCube(e[o]||vM,s[o])}function C1(n,e,t){let i=this.cache,r=e.length,s=Hd(t,r);qt(i,s)||(n.uniform1iv(this.addr,s),Xt(i,s));for(let o=0;o!==r;++o)t.setTexture2DArray(e[o]||mM,s[o])}function I1(n){switch(n){case 5126:return l1;case 35664:return u1;case 35665:return d1;case 35666:return f1;case 35674:return h1;case 35675:return p1;case 35676:return m1;case 5124:case 35670:return g1;case 35667:case 35671:return v1;case 35668:case 35672:return y1;case 35669:case 35673:return _1;case 5125:return x1;case 36294:return M1;case 36295:return b1;case 36296:return S1;case 35678:case 36198:case 36298:case 36306:case 35682:return E1;case 35679:case 36299:case 36307:return T1;case 35680:case 36300:case 36308:case 36293:return w1;case 36289:case 36303:case 36311:case 36292:return C1}}var yg=class{constructor(e,t,i){this.id=e,this.addr=i,this.cache=[],this.type=t.type,this.setValue=c1(t.type)}},_g=class{constructor(e,t,i){this.id=e,this.addr=i,this.cache=[],this.type=t.type,this.size=t.size,this.setValue=I1(t.type)}},xg=class{constructor(e){this.id=e,this.seq=[],this.map={}}setValue(e,t,i){let r=this.seq;for(let s=0,o=r.length;s!==o;++s){let a=r[s];a.setValue(e,t[a.id],i)}}},mg=/(\w+)(\])?(\[|\.)?/g;function tM(n,e){n.seq.push(e),n.map[e.id]=e}function A1(n,e,t){let i=n.name,r=i.length;for(mg.lastIndex=0;;){let s=mg.exec(i),o=mg.lastIndex,a=s[1],c=s[2]==="]",l=s[3];if(c&&(a=a|0),l===void 0||l==="["&&o+2===r){tM(t,l===void 0?new yg(a,n,e):new _g(a,n,e));break}else{let f=t.map[a];f===void 0&&(f=new xg(a),tM(t,f)),t=f}}}var Ro=class{constructor(e,t){this.seq=[],this.map={};let i=e.getProgramParameter(t,e.ACTIVE_UNIFORMS);for(let o=0;o<i;++o){let a=e.getActiveUniform(t,o),c=e.getUniformLocation(t,a.name);A1(a,c,this)}let r=[],s=[];for(let o of this.seq)o.type===e.SAMPLER_2D_SHADOW||o.type===e.SAMPLER_CUBE_SHADOW||o.type===e.SAMPLER_2D_ARRAY_SHADOW?r.push(o):s.push(o);r.length>0&&(this.seq=r.concat(s))}setValue(e,t,i,r){let s=this.map[t];s!==void 0&&s.setValue(e,i,r)}setOptional(e,t,i){let r=t[i];r!==void 0&&this.setValue(e,i,r)}static upload(e,t,i,r){for(let s=0,o=t.length;s!==o;++s){let a=t[s],c=i[a.id];c.needsUpdate!==!1&&a.setValue(e,c.value,r)}}static seqWithValue(e,t){let i=[];for(let r=0,s=e.length;r!==s;++r){let o=e[r];o.id in t&&i.push(o)}return i}};function nM(n,e,t){let i=n.createShader(e);return n.shaderSource(i,t),n.compileShader(i),i}var D1=37297,R1=0;function N1(n,e){let t=n.split(`
`),i=[],r=Math.max(e-6,0),s=Math.min(e+6,t.length);for(let o=r;o<s;o++){let a=o+1;i.push(`${a===e?">":" "} ${a}: ${t[o]}`)}return i.join(`
`)}var iM=new Be;function P1(n){tt._getMatrix(iM,tt.workingColorSpace,n);let e=`mat3( ${iM.elements.map(t=>t.toFixed(4))} )`;switch(tt.getTransfer(n)){case Aa:return[e,"LinearTransferOETF"];case ut:return[e,"sRGBTransferOETF"];default:return Ne("WebGLProgram: Unsupported color space: ",n),[e,"LinearTransferOETF"]}}function rM(n,e,t){let i=n.getShaderParameter(e,n.COMPILE_STATUS),s=(n.getShaderInfoLog(e)||"").trim();if(i&&s==="")return"";let o=/ERROR: 0:(\d+)/.exec(s);if(o){let a=parseInt(o[1]);return t.toUpperCase()+`

`+s+`

`+N1(n.getShaderSource(e),a)}else return s}function L1(n,e){let t=P1(e);return[`vec4 ${n}( vec4 value ) {`,`	return ${t[1]}( vec4( value.rgb * ${t[0]}, value.a ) );`,"}"].join(`
`)}var O1={[jm]:"Linear",[$m]:"Reinhard",[qm]:"Cineon",[Xm]:"ACESFilmic",[Zm]:"AgX",[Jm]:"Neutral",[Ym]:"Custom"};function F1(n,e){let t=O1[e];return t===void 0?(Ne("WebGLProgram: Unsupported toneMapping:",e),"vec3 "+n+"( vec3 color ) { return LinearToneMapping( color ); }"):"vec3 "+n+"( vec3 color ) { return "+t+"ToneMapping( color ); }"}var Fd=new k;function k1(){tt.getLuminanceCoefficients(Fd);let n=Fd.x.toFixed(4),e=Fd.y.toFixed(4),t=Fd.z.toFixed(4);return["float luminance( const in vec3 rgb ) {",`	const vec3 weights = vec3( ${n}, ${e}, ${t} );`,"	return dot( weights, rgb );","}"].join(`
`)}function U1(n){return[n.extensionClipCullDistance?"#extension GL_ANGLE_clip_cull_distance : require":"",n.extensionMultiDraw?"#extension GL_ANGLE_multi_draw : require":""].filter(sc).join(`
`)}function B1(n){let e=[];for(let t in n){let i=n[t];i!==!1&&e.push("#define "+t+" "+i)}return e.join(`
`)}function V1(n,e){let t={},i=n.getProgramParameter(e,n.ACTIVE_ATTRIBUTES);for(let r=0;r<i;r++){let s=n.getActiveAttrib(e,r),o=s.name,a=1;s.type===n.FLOAT_MAT2&&(a=2),s.type===n.FLOAT_MAT3&&(a=3),s.type===n.FLOAT_MAT4&&(a=4),t[o]={type:s.type,location:n.getAttribLocation(e,o),locationSize:a}}return t}function sc(n){return n!==""}function sM(n,e){let t=e.numSpotLightShadows+e.numSpotLightMaps-e.numSpotLightShadowsWithMaps;return n.replace(/NUM_DIR_LIGHTS/g,e.numDirLights).replace(/NUM_SPOT_LIGHTS/g,e.numSpotLights).replace(/NUM_SPOT_LIGHT_MAPS/g,e.numSpotLightMaps).replace(/NUM_SPOT_LIGHT_COORDS/g,t).replace(/NUM_RECT_AREA_LIGHTS/g,e.numRectAreaLights).replace(/NUM_POINT_LIGHTS/g,e.numPointLights).replace(/NUM_HEMI_LIGHTS/g,e.numHemiLights).replace(/NUM_DIR_LIGHT_SHADOWS/g,e.numDirLightShadows).replace(/NUM_SPOT_LIGHT_SHADOWS_WITH_MAPS/g,e.numSpotLightShadowsWithMaps).replace(/NUM_SPOT_LIGHT_SHADOWS/g,e.numSpotLightShadows).replace(/NUM_POINT_LIGHT_SHADOWS/g,e.numPointLightShadows)}function oM(n,e){return n.replace(/NUM_CLIPPING_PLANES/g,e.numClippingPlanes).replace(/UNION_CLIPPING_PLANES/g,e.numClippingPlanes-e.numClipIntersection)}var H1=/^[ \t]*#include +<([\w\d./]+)>/gm;function Mg(n){return n.replace(H1,G1)}var z1=new Map;function G1(n,e){let t=ze[e];if(t===void 0){let i=z1.get(e);if(i!==void 0)t=ze[i],Ne('WebGLRenderer: Shader chunk "%s" has been deprecated. Use "%s" instead.',e,i);else throw new Error("Can not resolve #include <"+e+">")}return Mg(t)}var W1=/#pragma unroll_loop_start\s+for\s*\(\s*int\s+i\s*=\s*(\d+)\s*;\s*i\s*<\s*(\d+)\s*;\s*i\s*\+\+\s*\)\s*{([\s\S]+?)}\s+#pragma unroll_loop_end/g;function aM(n){return n.replace(W1,j1)}function j1(n,e,t,i){let r="";for(let s=parseInt(e);s<parseInt(t);s++)r+=i.replace(/\[\s*i\s*\]/g,"[ "+s+" ]").replace(/UNROLLED_LOOP_INDEX/g,s);return r}function cM(n){let e=`precision ${n.precision} float;
	precision ${n.precision} int;
	precision ${n.precision} sampler2D;
	precision ${n.precision} samplerCube;
	precision ${n.precision} sampler3D;
	precision ${n.precision} sampler2DArray;
	precision ${n.precision} sampler2DShadow;
	precision ${n.precision} samplerCubeShadow;
	precision ${n.precision} sampler2DArrayShadow;
	precision ${n.precision} isampler2D;
	precision ${n.precision} isampler3D;
	precision ${n.precision} isamplerCube;
	precision ${n.precision} isampler2DArray;
	precision ${n.precision} usampler2D;
	precision ${n.precision} usampler3D;
	precision ${n.precision} usamplerCube;
	precision ${n.precision} usampler2DArray;
	`;return n.precision==="highp"?e+=`
#define HIGH_PRECISION`:n.precision==="mediump"?e+=`
#define MEDIUM_PRECISION`:n.precision==="lowp"&&(e+=`
#define LOW_PRECISION`),e}var $1={[Ya]:"SHADOWMAP_TYPE_PCF",[Co]:"SHADOWMAP_TYPE_VSM"};function q1(n){return $1[n.shadowMapType]||"SHADOWMAP_TYPE_BASIC"}var X1={[Lr]:"ENVMAP_TYPE_CUBE",[Es]:"ENVMAP_TYPE_CUBE",[Za]:"ENVMAP_TYPE_CUBE_UV"};function Y1(n){return n.envMap===!1?"ENVMAP_TYPE_CUBE":X1[n.envMapMode]||"ENVMAP_TYPE_CUBE"}var Z1={[Es]:"ENVMAP_MODE_REFRACTION"};function J1(n){return n.envMap===!1?"ENVMAP_MODE_REFLECTION":Z1[n.envMapMode]||"ENVMAP_MODE_REFLECTION"}var K1={[Wm]:"ENVMAP_BLENDING_MULTIPLY",[C0]:"ENVMAP_BLENDING_MIX",[I0]:"ENVMAP_BLENDING_ADD"};function Q1(n){return n.envMap===!1?"ENVMAP_BLENDING_NONE":K1[n.combine]||"ENVMAP_BLENDING_NONE"}function eR(n){let e=n.envMapCubeUVHeight;if(e===null)return null;let t=Math.log2(e)-2,i=1/e;return{texelWidth:1/(3*Math.max(Math.pow(2,t),112)),texelHeight:i,maxMip:t}}function tR(n,e,t,i){let r=n.getContext(),s=t.defines,o=t.vertexShader,a=t.fragmentShader,c=q1(t),l=Y1(t),u=J1(t),f=Q1(t),d=eR(t),h=U1(t),g=B1(s),x=r.createProgram(),m,p,M=t.glslVersion?"#version "+t.glslVersion+`
`:"";t.isRawShaderMaterial?(m=["#define SHADER_TYPE "+t.shaderType,"#define SHADER_NAME "+t.shaderName,g].filter(sc).join(`
`),m.length>0&&(m+=`
`),p=["#define SHADER_TYPE "+t.shaderType,"#define SHADER_NAME "+t.shaderName,g].filter(sc).join(`
`),p.length>0&&(p+=`
`)):(m=[cM(t),"#define SHADER_TYPE "+t.shaderType,"#define SHADER_NAME "+t.shaderName,g,t.extensionClipCullDistance?"#define USE_CLIP_DISTANCE":"",t.batching?"#define USE_BATCHING":"",t.batchingColor?"#define USE_BATCHING_COLOR":"",t.instancing?"#define USE_INSTANCING":"",t.instancingColor?"#define USE_INSTANCING_COLOR":"",t.instancingMorph?"#define USE_INSTANCING_MORPH":"",t.useFog&&t.fog?"#define USE_FOG":"",t.useFog&&t.fogExp2?"#define FOG_EXP2":"",t.map?"#define USE_MAP":"",t.envMap?"#define USE_ENVMAP":"",t.envMap?"#define "+u:"",t.lightMap?"#define USE_LIGHTMAP":"",t.aoMap?"#define USE_AOMAP":"",t.bumpMap?"#define USE_BUMPMAP":"",t.normalMap?"#define USE_NORMALMAP":"",t.normalMapObjectSpace?"#define USE_NORMALMAP_OBJECTSPACE":"",t.normalMapTangentSpace?"#define USE_NORMALMAP_TANGENTSPACE":"",t.displacementMap?"#define USE_DISPLACEMENTMAP":"",t.emissiveMap?"#define USE_EMISSIVEMAP":"",t.anisotropy?"#define USE_ANISOTROPY":"",t.anisotropyMap?"#define USE_ANISOTROPYMAP":"",t.clearcoatMap?"#define USE_CLEARCOATMAP":"",t.clearcoatRoughnessMap?"#define USE_CLEARCOAT_ROUGHNESSMAP":"",t.clearcoatNormalMap?"#define USE_CLEARCOAT_NORMALMAP":"",t.iridescenceMap?"#define USE_IRIDESCENCEMAP":"",t.iridescenceThicknessMap?"#define USE_IRIDESCENCE_THICKNESSMAP":"",t.specularMap?"#define USE_SPECULARMAP":"",t.specularColorMap?"#define USE_SPECULAR_COLORMAP":"",t.specularIntensityMap?"#define USE_SPECULAR_INTENSITYMAP":"",t.roughnessMap?"#define USE_ROUGHNESSMAP":"",t.metalnessMap?"#define USE_METALNESSMAP":"",t.alphaMap?"#define USE_ALPHAMAP":"",t.alphaHash?"#define USE_ALPHAHASH":"",t.transmission?"#define USE_TRANSMISSION":"",t.transmissionMap?"#define USE_TRANSMISSIONMAP":"",t.thicknessMap?"#define USE_THICKNESSMAP":"",t.sheenColorMap?"#define USE_SHEEN_COLORMAP":"",t.sheenRoughnessMap?"#define USE_SHEEN_ROUGHNESSMAP":"",t.mapUv?"#define MAP_UV "+t.mapUv:"",t.alphaMapUv?"#define ALPHAMAP_UV "+t.alphaMapUv:"",t.lightMapUv?"#define LIGHTMAP_UV "+t.lightMapUv:"",t.aoMapUv?"#define AOMAP_UV "+t.aoMapUv:"",t.emissiveMapUv?"#define EMISSIVEMAP_UV "+t.emissiveMapUv:"",t.bumpMapUv?"#define BUMPMAP_UV "+t.bumpMapUv:"",t.normalMapUv?"#define NORMALMAP_UV "+t.normalMapUv:"",t.displacementMapUv?"#define DISPLACEMENTMAP_UV "+t.displacementMapUv:"",t.metalnessMapUv?"#define METALNESSMAP_UV "+t.metalnessMapUv:"",t.roughnessMapUv?"#define ROUGHNESSMAP_UV "+t.roughnessMapUv:"",t.anisotropyMapUv?"#define ANISOTROPYMAP_UV "+t.anisotropyMapUv:"",t.clearcoatMapUv?"#define CLEARCOATMAP_UV "+t.clearcoatMapUv:"",t.clearcoatNormalMapUv?"#define CLEARCOAT_NORMALMAP_UV "+t.clearcoatNormalMapUv:"",t.clearcoatRoughnessMapUv?"#define CLEARCOAT_ROUGHNESSMAP_UV "+t.clearcoatRoughnessMapUv:"",t.iridescenceMapUv?"#define IRIDESCENCEMAP_UV "+t.iridescenceMapUv:"",t.iridescenceThicknessMapUv?"#define IRIDESCENCE_THICKNESSMAP_UV "+t.iridescenceThicknessMapUv:"",t.sheenColorMapUv?"#define SHEEN_COLORMAP_UV "+t.sheenColorMapUv:"",t.sheenRoughnessMapUv?"#define SHEEN_ROUGHNESSMAP_UV "+t.sheenRoughnessMapUv:"",t.specularMapUv?"#define SPECULARMAP_UV "+t.specularMapUv:"",t.specularColorMapUv?"#define SPECULAR_COLORMAP_UV "+t.specularColorMapUv:"",t.specularIntensityMapUv?"#define SPECULAR_INTENSITYMAP_UV "+t.specularIntensityMapUv:"",t.transmissionMapUv?"#define TRANSMISSIONMAP_UV "+t.transmissionMapUv:"",t.thicknessMapUv?"#define THICKNESSMAP_UV "+t.thicknessMapUv:"",t.vertexTangents&&t.flatShading===!1?"#define USE_TANGENT":"",t.vertexColors?"#define USE_COLOR":"",t.vertexAlphas?"#define USE_COLOR_ALPHA":"",t.vertexUv1s?"#define USE_UV1":"",t.vertexUv2s?"#define USE_UV2":"",t.vertexUv3s?"#define USE_UV3":"",t.pointsUvs?"#define USE_POINTS_UV":"",t.flatShading?"#define FLAT_SHADED":"",t.skinning?"#define USE_SKINNING":"",t.morphTargets?"#define USE_MORPHTARGETS":"",t.morphNormals&&t.flatShading===!1?"#define USE_MORPHNORMALS":"",t.morphColors?"#define USE_MORPHCOLORS":"",t.morphTargetsCount>0?"#define MORPHTARGETS_TEXTURE_STRIDE "+t.morphTextureStride:"",t.morphTargetsCount>0?"#define MORPHTARGETS_COUNT "+t.morphTargetsCount:"",t.doubleSided?"#define DOUBLE_SIDED":"",t.flipSided?"#define FLIP_SIDED":"",t.shadowMapEnabled?"#define USE_SHADOWMAP":"",t.shadowMapEnabled?"#define "+c:"",t.sizeAttenuation?"#define USE_SIZEATTENUATION":"",t.numLightProbes>0?"#define USE_LIGHT_PROBES":"",t.logarithmicDepthBuffer?"#define USE_LOGARITHMIC_DEPTH_BUFFER":"",t.reversedDepthBuffer?"#define USE_REVERSED_DEPTH_BUFFER":"","uniform mat4 modelMatrix;","uniform mat4 modelViewMatrix;","uniform mat4 projectionMatrix;","uniform mat4 viewMatrix;","uniform mat3 normalMatrix;","uniform vec3 cameraPosition;","uniform bool isOrthographic;","#ifdef USE_INSTANCING","	attribute mat4 instanceMatrix;","#endif","#ifdef USE_INSTANCING_COLOR","	attribute vec3 instanceColor;","#endif","#ifdef USE_INSTANCING_MORPH","	uniform sampler2D morphTexture;","#endif","attribute vec3 position;","attribute vec3 normal;","attribute vec2 uv;","#ifdef USE_UV1","	attribute vec2 uv1;","#endif","#ifdef USE_UV2","	attribute vec2 uv2;","#endif","#ifdef USE_UV3","	attribute vec2 uv3;","#endif","#ifdef USE_TANGENT","	attribute vec4 tangent;","#endif","#if defined( USE_COLOR_ALPHA )","	attribute vec4 color;","#elif defined( USE_COLOR )","	attribute vec3 color;","#endif","#ifdef USE_SKINNING","	attribute vec4 skinIndex;","	attribute vec4 skinWeight;","#endif",`
`].filter(sc).join(`
`),p=[cM(t),"#define SHADER_TYPE "+t.shaderType,"#define SHADER_NAME "+t.shaderName,g,t.useFog&&t.fog?"#define USE_FOG":"",t.useFog&&t.fogExp2?"#define FOG_EXP2":"",t.alphaToCoverage?"#define ALPHA_TO_COVERAGE":"",t.map?"#define USE_MAP":"",t.matcap?"#define USE_MATCAP":"",t.envMap?"#define USE_ENVMAP":"",t.envMap?"#define "+l:"",t.envMap?"#define "+u:"",t.envMap?"#define "+f:"",d?"#define CUBEUV_TEXEL_WIDTH "+d.texelWidth:"",d?"#define CUBEUV_TEXEL_HEIGHT "+d.texelHeight:"",d?"#define CUBEUV_MAX_MIP "+d.maxMip+".0":"",t.lightMap?"#define USE_LIGHTMAP":"",t.aoMap?"#define USE_AOMAP":"",t.bumpMap?"#define USE_BUMPMAP":"",t.normalMap?"#define USE_NORMALMAP":"",t.normalMapObjectSpace?"#define USE_NORMALMAP_OBJECTSPACE":"",t.normalMapTangentSpace?"#define USE_NORMALMAP_TANGENTSPACE":"",t.emissiveMap?"#define USE_EMISSIVEMAP":"",t.anisotropy?"#define USE_ANISOTROPY":"",t.anisotropyMap?"#define USE_ANISOTROPYMAP":"",t.clearcoat?"#define USE_CLEARCOAT":"",t.clearcoatMap?"#define USE_CLEARCOATMAP":"",t.clearcoatRoughnessMap?"#define USE_CLEARCOAT_ROUGHNESSMAP":"",t.clearcoatNormalMap?"#define USE_CLEARCOAT_NORMALMAP":"",t.dispersion?"#define USE_DISPERSION":"",t.iridescence?"#define USE_IRIDESCENCE":"",t.iridescenceMap?"#define USE_IRIDESCENCEMAP":"",t.iridescenceThicknessMap?"#define USE_IRIDESCENCE_THICKNESSMAP":"",t.specularMap?"#define USE_SPECULARMAP":"",t.specularColorMap?"#define USE_SPECULAR_COLORMAP":"",t.specularIntensityMap?"#define USE_SPECULAR_INTENSITYMAP":"",t.roughnessMap?"#define USE_ROUGHNESSMAP":"",t.metalnessMap?"#define USE_METALNESSMAP":"",t.alphaMap?"#define USE_ALPHAMAP":"",t.alphaTest?"#define USE_ALPHATEST":"",t.alphaHash?"#define USE_ALPHAHASH":"",t.sheen?"#define USE_SHEEN":"",t.sheenColorMap?"#define USE_SHEEN_COLORMAP":"",t.sheenRoughnessMap?"#define USE_SHEEN_ROUGHNESSMAP":"",t.transmission?"#define USE_TRANSMISSION":"",t.transmissionMap?"#define USE_TRANSMISSIONMAP":"",t.thicknessMap?"#define USE_THICKNESSMAP":"",t.vertexTangents&&t.flatShading===!1?"#define USE_TANGENT":"",t.vertexColors||t.instancingColor?"#define USE_COLOR":"",t.vertexAlphas||t.batchingColor?"#define USE_COLOR_ALPHA":"",t.vertexUv1s?"#define USE_UV1":"",t.vertexUv2s?"#define USE_UV2":"",t.vertexUv3s?"#define USE_UV3":"",t.pointsUvs?"#define USE_POINTS_UV":"",t.gradientMap?"#define USE_GRADIENTMAP":"",t.flatShading?"#define FLAT_SHADED":"",t.doubleSided?"#define DOUBLE_SIDED":"",t.flipSided?"#define FLIP_SIDED":"",t.shadowMapEnabled?"#define USE_SHADOWMAP":"",t.shadowMapEnabled?"#define "+c:"",t.premultipliedAlpha?"#define PREMULTIPLIED_ALPHA":"",t.numLightProbes>0?"#define USE_LIGHT_PROBES":"",t.decodeVideoTexture?"#define DECODE_VIDEO_TEXTURE":"",t.decodeVideoTextureEmissive?"#define DECODE_VIDEO_TEXTURE_EMISSIVE":"",t.logarithmicDepthBuffer?"#define USE_LOGARITHMIC_DEPTH_BUFFER":"",t.reversedDepthBuffer?"#define USE_REVERSED_DEPTH_BUFFER":"","uniform mat4 viewMatrix;","uniform vec3 cameraPosition;","uniform bool isOrthographic;",t.toneMapping!==di?"#define TONE_MAPPING":"",t.toneMapping!==di?ze.tonemapping_pars_fragment:"",t.toneMapping!==di?F1("toneMapping",t.toneMapping):"",t.dithering?"#define DITHERING":"",t.opaque?"#define OPAQUE":"",ze.colorspace_pars_fragment,L1("linearToOutputTexel",t.outputColorSpace),k1(),t.useDepthPacking?"#define DEPTH_PACKING "+t.depthPacking:"",`
`].filter(sc).join(`
`)),o=Mg(o),o=sM(o,t),o=oM(o,t),a=Mg(a),a=sM(a,t),a=oM(a,t),o=aM(o),a=aM(a),t.isRawShaderMaterial!==!0&&(M=`#version 300 es
`,m=[h,"#define attribute in","#define varying out","#define texture2D texture"].join(`
`)+`
`+m,p=["#define varying in",t.glslVersion===sg?"":"layout(location = 0) out highp vec4 pc_fragColor;",t.glslVersion===sg?"":"#define gl_FragColor pc_fragColor","#define gl_FragDepthEXT gl_FragDepth","#define texture2D texture","#define textureCube texture","#define texture2DProj textureProj","#define texture2DLodEXT textureLod","#define texture2DProjLodEXT textureProjLod","#define textureCubeLodEXT textureLod","#define texture2DGradEXT textureGrad","#define texture2DProjGradEXT textureProjGrad","#define textureCubeGradEXT textureGrad"].join(`
`)+`
`+p);let T=M+m+o,E=M+p+a,A=nM(r,r.VERTEX_SHADER,T),I=nM(r,r.FRAGMENT_SHADER,E);r.attachShader(x,A),r.attachShader(x,I),t.index0AttributeName!==void 0?r.bindAttribLocation(x,0,t.index0AttributeName):t.morphTargets===!0&&r.bindAttribLocation(x,0,"position"),r.linkProgram(x);function D(w){if(n.debug.checkShaderErrors){let U=r.getProgramInfoLog(x)||"",V=r.getShaderInfoLog(A)||"",W=r.getShaderInfoLog(I)||"",B=U.trim(),H=V.trim(),L=W.trim(),Q=!0,Z=!0;if(r.getProgramParameter(x,r.LINK_STATUS)===!1)if(Q=!1,typeof n.debug.onShaderError=="function")n.debug.onShaderError(r,x,A,I);else{let ue=rM(r,A,"vertex"),ge=rM(r,I,"fragment");Re("THREE.WebGLProgram: Shader Error "+r.getError()+" - VALIDATE_STATUS "+r.getProgramParameter(x,r.VALIDATE_STATUS)+`

Material Name: `+w.name+`
Material Type: `+w.type+`

Program Info Log: `+B+`
`+ue+`
`+ge)}else B!==""?Ne("WebGLProgram: Program Info Log:",B):(H===""||L==="")&&(Z=!1);Z&&(w.diagnostics={runnable:Q,programLog:B,vertexShader:{log:H,prefix:m},fragmentShader:{log:L,prefix:p}})}r.deleteShader(A),r.deleteShader(I),y=new Ro(r,x),S=V1(r,x)}let y;this.getUniforms=function(){return y===void 0&&D(this),y};let S;this.getAttributes=function(){return S===void 0&&D(this),S};let q=t.rendererExtensionParallelShaderCompile===!1;return this.isReady=function(){return q===!1&&(q=r.getProgramParameter(x,D1)),q},this.destroy=function(){i.releaseStatesOfProgram(this),r.deleteProgram(x),this.program=void 0},this.type=t.shaderType,this.name=t.shaderName,this.id=R1++,this.cacheKey=e,this.usedTimes=1,this.program=x,this.vertexShader=A,this.fragmentShader=I,this}var nR=0,bg=class{constructor(){this.shaderCache=new Map,this.materialCache=new Map}update(e){let t=e.vertexShader,i=e.fragmentShader,r=this._getShaderStage(t),s=this._getShaderStage(i),o=this._getShaderCacheForMaterial(e);return o.has(r)===!1&&(o.add(r),r.usedTimes++),o.has(s)===!1&&(o.add(s),s.usedTimes++),this}remove(e){let t=this.materialCache.get(e);for(let i of t)i.usedTimes--,i.usedTimes===0&&this.shaderCache.delete(i.code);return this.materialCache.delete(e),this}getVertexShaderID(e){return this._getShaderStage(e.vertexShader).id}getFragmentShaderID(e){return this._getShaderStage(e.fragmentShader).id}dispose(){this.shaderCache.clear(),this.materialCache.clear()}_getShaderCacheForMaterial(e){let t=this.materialCache,i=t.get(e);return i===void 0&&(i=new Set,t.set(e,i)),i}_getShaderStage(e){let t=this.shaderCache,i=t.get(e);return i===void 0&&(i=new Sg(e),t.set(e,i)),i}},Sg=class{constructor(e){this.id=nR++,this.code=e,this.usedTimes=0}};function iR(n,e,t,i,r,s){let o=new La,a=new bg,c=new Set,l=[],u=new Map,f=i.logarithmicDepthBuffer,d=i.precision,h={MeshDepthMaterial:"depth",MeshDistanceMaterial:"distance",MeshNormalMaterial:"normal",MeshBasicMaterial:"basic",MeshLambertMaterial:"lambert",MeshPhongMaterial:"phong",MeshToonMaterial:"toon",MeshStandardMaterial:"physical",MeshPhysicalMaterial:"physical",MeshMatcapMaterial:"matcap",LineBasicMaterial:"basic",LineDashedMaterial:"dashed",PointsMaterial:"points",ShadowMaterial:"shadow",SpriteMaterial:"sprite"};function g(y){return c.add(y),y===0?"uv":`uv${y}`}function x(y,S,q,w,U){let V=w.fog,W=U.geometry,B=y.isMeshStandardMaterial||y.isMeshLambertMaterial||y.isMeshPhongMaterial?w.environment:null,H=y.isMeshStandardMaterial||y.isMeshLambertMaterial&&!y.envMap||y.isMeshPhongMaterial&&!y.envMap,L=e.get(y.envMap||B,H),Q=L&&L.mapping===Za?L.image.height:null,Z=h[y.type];y.precision!==null&&(d=i.getMaxPrecision(y.precision),d!==y.precision&&Ne("WebGLProgram.getParameters:",y.precision,"not supported, using",d,"instead."));let ue=W.morphAttributes.position||W.morphAttributes.normal||W.morphAttributes.color,ge=ue!==void 0?ue.length:0,fe=0;W.morphAttributes.position!==void 0&&(fe=1),W.morphAttributes.normal!==void 0&&(fe=2),W.morphAttributes.color!==void 0&&(fe=3);let He,Tt,St,X;if(Z){let ft=Ni[Z];He=ft.vertexShader,Tt=ft.fragmentShader}else He=y.vertexShader,Tt=y.fragmentShader,a.update(y),St=a.getVertexShaderID(y),X=a.getFragmentShaderID(y);let ne=n.getRenderTarget(),se=n.state.buffers.depth.getReversed(),Ve=U.isInstancedMesh===!0,Ie=U.isBatchedMesh===!0,Pe=!!y.map,Yt=!!y.matcap,nt=!!L,dt=!!y.aoMap,yt=!!y.lightMap,Ge=!!y.bumpMap,Lt=!!y.normalMap,C=!!y.displacementMap,Bt=!!y.emissiveMap,lt=!!y.metalnessMap,xt=!!y.roughnessMap,be=y.anisotropy>0,b=y.clearcoat>0,v=y.dispersion>0,N=y.iridescence>0,$=y.sheen>0,Y=y.transmission>0,j=be&&!!y.anisotropyMap,ve=b&&!!y.clearcoatMap,ie=b&&!!y.clearcoatNormalMap,we=b&&!!y.clearcoatRoughnessMap,De=N&&!!y.iridescenceMap,J=N&&!!y.iridescenceThicknessMap,ee=$&&!!y.sheenColorMap,ye=$&&!!y.sheenRoughnessMap,xe=!!y.specularMap,de=!!y.specularColorMap,We=!!y.specularIntensityMap,R=Y&&!!y.transmissionMap,re=Y&&!!y.thicknessMap,te=!!y.gradientMap,pe=!!y.alphaMap,K=y.alphaTest>0,G=!!y.alphaHash,_e=!!y.extensions,Fe=di;y.toneMapped&&(ne===null||ne.isXRRenderTarget===!0)&&(Fe=n.toneMapping);let Mt={shaderID:Z,shaderType:y.type,shaderName:y.name,vertexShader:He,fragmentShader:Tt,defines:y.defines,customVertexShaderID:St,customFragmentShaderID:X,isRawShaderMaterial:y.isRawShaderMaterial===!0,glslVersion:y.glslVersion,precision:d,batching:Ie,batchingColor:Ie&&U._colorsTexture!==null,instancing:Ve,instancingColor:Ve&&U.instanceColor!==null,instancingMorph:Ve&&U.morphTexture!==null,outputColorSpace:ne===null?n.outputColorSpace:ne.isXRRenderTarget===!0?ne.texture.colorSpace:Ms,alphaToCoverage:!!y.alphaToCoverage,map:Pe,matcap:Yt,envMap:nt,envMapMode:nt&&L.mapping,envMapCubeUVHeight:Q,aoMap:dt,lightMap:yt,bumpMap:Ge,normalMap:Lt,displacementMap:C,emissiveMap:Bt,normalMapObjectSpace:Lt&&y.normalMapType===N0,normalMapTangentSpace:Lt&&y.normalMapType===R0,metalnessMap:lt,roughnessMap:xt,anisotropy:be,anisotropyMap:j,clearcoat:b,clearcoatMap:ve,clearcoatNormalMap:ie,clearcoatRoughnessMap:we,dispersion:v,iridescence:N,iridescenceMap:De,iridescenceThicknessMap:J,sheen:$,sheenColorMap:ee,sheenRoughnessMap:ye,specularMap:xe,specularColorMap:de,specularIntensityMap:We,transmission:Y,transmissionMap:R,thicknessMap:re,gradientMap:te,opaque:y.transparent===!1&&y.blending===tr&&y.alphaToCoverage===!1,alphaMap:pe,alphaTest:K,alphaHash:G,combine:y.combine,mapUv:Pe&&g(y.map.channel),aoMapUv:dt&&g(y.aoMap.channel),lightMapUv:yt&&g(y.lightMap.channel),bumpMapUv:Ge&&g(y.bumpMap.channel),normalMapUv:Lt&&g(y.normalMap.channel),displacementMapUv:C&&g(y.displacementMap.channel),emissiveMapUv:Bt&&g(y.emissiveMap.channel),metalnessMapUv:lt&&g(y.metalnessMap.channel),roughnessMapUv:xt&&g(y.roughnessMap.channel),anisotropyMapUv:j&&g(y.anisotropyMap.channel),clearcoatMapUv:ve&&g(y.clearcoatMap.channel),clearcoatNormalMapUv:ie&&g(y.clearcoatNormalMap.channel),clearcoatRoughnessMapUv:we&&g(y.clearcoatRoughnessMap.channel),iridescenceMapUv:De&&g(y.iridescenceMap.channel),iridescenceThicknessMapUv:J&&g(y.iridescenceThicknessMap.channel),sheenColorMapUv:ee&&g(y.sheenColorMap.channel),sheenRoughnessMapUv:ye&&g(y.sheenRoughnessMap.channel),specularMapUv:xe&&g(y.specularMap.channel),specularColorMapUv:de&&g(y.specularColorMap.channel),specularIntensityMapUv:We&&g(y.specularIntensityMap.channel),transmissionMapUv:R&&g(y.transmissionMap.channel),thicknessMapUv:re&&g(y.thicknessMap.channel),alphaMapUv:pe&&g(y.alphaMap.channel),vertexTangents:!!W.attributes.tangent&&(Lt||be),vertexColors:y.vertexColors,vertexAlphas:y.vertexColors===!0&&!!W.attributes.color&&W.attributes.color.itemSize===4,pointsUvs:U.isPoints===!0&&!!W.attributes.uv&&(Pe||pe),fog:!!V,useFog:y.fog===!0,fogExp2:!!V&&V.isFogExp2,flatShading:y.wireframe===!1&&(y.flatShading===!0||W.attributes.normal===void 0&&Lt===!1&&(y.isMeshLambertMaterial||y.isMeshPhongMaterial||y.isMeshStandardMaterial||y.isMeshPhysicalMaterial)),sizeAttenuation:y.sizeAttenuation===!0,logarithmicDepthBuffer:f,reversedDepthBuffer:se,skinning:U.isSkinnedMesh===!0,morphTargets:W.morphAttributes.position!==void 0,morphNormals:W.morphAttributes.normal!==void 0,morphColors:W.morphAttributes.color!==void 0,morphTargetsCount:ge,morphTextureStride:fe,numDirLights:S.directional.length,numPointLights:S.point.length,numSpotLights:S.spot.length,numSpotLightMaps:S.spotLightMap.length,numRectAreaLights:S.rectArea.length,numHemiLights:S.hemi.length,numDirLightShadows:S.directionalShadowMap.length,numPointLightShadows:S.pointShadowMap.length,numSpotLightShadows:S.spotShadowMap.length,numSpotLightShadowsWithMaps:S.numSpotLightShadowsWithMaps,numLightProbes:S.numLightProbes,numClippingPlanes:s.numPlanes,numClipIntersection:s.numIntersection,dithering:y.dithering,shadowMapEnabled:n.shadowMap.enabled&&q.length>0,shadowMapType:n.shadowMap.type,toneMapping:Fe,decodeVideoTexture:Pe&&y.map.isVideoTexture===!0&&tt.getTransfer(y.map.colorSpace)===ut,decodeVideoTextureEmissive:Bt&&y.emissiveMap.isVideoTexture===!0&&tt.getTransfer(y.emissiveMap.colorSpace)===ut,premultipliedAlpha:y.premultipliedAlpha,doubleSided:y.side===Ii,flipSided:y.side===vn,useDepthPacking:y.depthPacking>=0,depthPacking:y.depthPacking||0,index0AttributeName:y.index0AttributeName,extensionClipCullDistance:_e&&y.extensions.clipCullDistance===!0&&t.has("WEBGL_clip_cull_distance"),extensionMultiDraw:(_e&&y.extensions.multiDraw===!0||Ie)&&t.has("WEBGL_multi_draw"),rendererExtensionParallelShaderCompile:t.has("KHR_parallel_shader_compile"),customProgramCacheKey:y.customProgramCacheKey()};return Mt.vertexUv1s=c.has(1),Mt.vertexUv2s=c.has(2),Mt.vertexUv3s=c.has(3),c.clear(),Mt}function m(y){let S=[];if(y.shaderID?S.push(y.shaderID):(S.push(y.customVertexShaderID),S.push(y.customFragmentShaderID)),y.defines!==void 0)for(let q in y.defines)S.push(q),S.push(y.defines[q]);return y.isRawShaderMaterial===!1&&(p(S,y),M(S,y),S.push(n.outputColorSpace)),S.push(y.customProgramCacheKey),S.join()}function p(y,S){y.push(S.precision),y.push(S.outputColorSpace),y.push(S.envMapMode),y.push(S.envMapCubeUVHeight),y.push(S.mapUv),y.push(S.alphaMapUv),y.push(S.lightMapUv),y.push(S.aoMapUv),y.push(S.bumpMapUv),y.push(S.normalMapUv),y.push(S.displacementMapUv),y.push(S.emissiveMapUv),y.push(S.metalnessMapUv),y.push(S.roughnessMapUv),y.push(S.anisotropyMapUv),y.push(S.clearcoatMapUv),y.push(S.clearcoatNormalMapUv),y.push(S.clearcoatRoughnessMapUv),y.push(S.iridescenceMapUv),y.push(S.iridescenceThicknessMapUv),y.push(S.sheenColorMapUv),y.push(S.sheenRoughnessMapUv),y.push(S.specularMapUv),y.push(S.specularColorMapUv),y.push(S.specularIntensityMapUv),y.push(S.transmissionMapUv),y.push(S.thicknessMapUv),y.push(S.combine),y.push(S.fogExp2),y.push(S.sizeAttenuation),y.push(S.morphTargetsCount),y.push(S.morphAttributeCount),y.push(S.numDirLights),y.push(S.numPointLights),y.push(S.numSpotLights),y.push(S.numSpotLightMaps),y.push(S.numHemiLights),y.push(S.numRectAreaLights),y.push(S.numDirLightShadows),y.push(S.numPointLightShadows),y.push(S.numSpotLightShadows),y.push(S.numSpotLightShadowsWithMaps),y.push(S.numLightProbes),y.push(S.shadowMapType),y.push(S.toneMapping),y.push(S.numClippingPlanes),y.push(S.numClipIntersection),y.push(S.depthPacking)}function M(y,S){o.disableAll(),S.instancing&&o.enable(0),S.instancingColor&&o.enable(1),S.instancingMorph&&o.enable(2),S.matcap&&o.enable(3),S.envMap&&o.enable(4),S.normalMapObjectSpace&&o.enable(5),S.normalMapTangentSpace&&o.enable(6),S.clearcoat&&o.enable(7),S.iridescence&&o.enable(8),S.alphaTest&&o.enable(9),S.vertexColors&&o.enable(10),S.vertexAlphas&&o.enable(11),S.vertexUv1s&&o.enable(12),S.vertexUv2s&&o.enable(13),S.vertexUv3s&&o.enable(14),S.vertexTangents&&o.enable(15),S.anisotropy&&o.enable(16),S.alphaHash&&o.enable(17),S.batching&&o.enable(18),S.dispersion&&o.enable(19),S.batchingColor&&o.enable(20),S.gradientMap&&o.enable(21),y.push(o.mask),o.disableAll(),S.fog&&o.enable(0),S.useFog&&o.enable(1),S.flatShading&&o.enable(2),S.logarithmicDepthBuffer&&o.enable(3),S.reversedDepthBuffer&&o.enable(4),S.skinning&&o.enable(5),S.morphTargets&&o.enable(6),S.morphNormals&&o.enable(7),S.morphColors&&o.enable(8),S.premultipliedAlpha&&o.enable(9),S.shadowMapEnabled&&o.enable(10),S.doubleSided&&o.enable(11),S.flipSided&&o.enable(12),S.useDepthPacking&&o.enable(13),S.dithering&&o.enable(14),S.transmission&&o.enable(15),S.sheen&&o.enable(16),S.opaque&&o.enable(17),S.pointsUvs&&o.enable(18),S.decodeVideoTexture&&o.enable(19),S.decodeVideoTextureEmissive&&o.enable(20),S.alphaToCoverage&&o.enable(21),y.push(o.mask)}function T(y){let S=h[y.type],q;if(S){let w=Ni[S];q=W0.clone(w.uniforms)}else q=y.uniforms;return q}function E(y,S){let q=u.get(S);return q!==void 0?++q.usedTimes:(q=new tR(n,S,y,r),l.push(q),u.set(S,q)),q}function A(y){if(--y.usedTimes===0){let S=l.indexOf(y);l[S]=l[l.length-1],l.pop(),u.delete(y.cacheKey),y.destroy()}}function I(y){a.remove(y)}function D(){a.dispose()}return{getParameters:x,getProgramCacheKey:m,getUniforms:T,acquireProgram:E,releaseProgram:A,releaseShaderCache:I,programs:l,dispose:D}}function rR(){let n=new WeakMap;function e(o){return n.has(o)}function t(o){let a=n.get(o);return a===void 0&&(a={},n.set(o,a)),a}function i(o){n.delete(o)}function r(o,a,c){n.get(o)[a]=c}function s(){n=new WeakMap}return{has:e,get:t,remove:i,update:r,dispose:s}}function sR(n,e){return n.groupOrder!==e.groupOrder?n.groupOrder-e.groupOrder:n.renderOrder!==e.renderOrder?n.renderOrder-e.renderOrder:n.material.id!==e.material.id?n.material.id-e.material.id:n.materialVariant!==e.materialVariant?n.materialVariant-e.materialVariant:n.z!==e.z?n.z-e.z:n.id-e.id}function lM(n,e){return n.groupOrder!==e.groupOrder?n.groupOrder-e.groupOrder:n.renderOrder!==e.renderOrder?n.renderOrder-e.renderOrder:n.z!==e.z?e.z-n.z:n.id-e.id}function uM(){let n=[],e=0,t=[],i=[],r=[];function s(){e=0,t.length=0,i.length=0,r.length=0}function o(d){let h=0;return d.isInstancedMesh&&(h+=2),d.isSkinnedMesh&&(h+=1),h}function a(d,h,g,x,m,p){let M=n[e];return M===void 0?(M={id:d.id,object:d,geometry:h,material:g,materialVariant:o(d),groupOrder:x,renderOrder:d.renderOrder,z:m,group:p},n[e]=M):(M.id=d.id,M.object=d,M.geometry=h,M.material=g,M.materialVariant=o(d),M.groupOrder=x,M.renderOrder=d.renderOrder,M.z=m,M.group=p),e++,M}function c(d,h,g,x,m,p){let M=a(d,h,g,x,m,p);g.transmission>0?i.push(M):g.transparent===!0?r.push(M):t.push(M)}function l(d,h,g,x,m,p){let M=a(d,h,g,x,m,p);g.transmission>0?i.unshift(M):g.transparent===!0?r.unshift(M):t.unshift(M)}function u(d,h){t.length>1&&t.sort(d||sR),i.length>1&&i.sort(h||lM),r.length>1&&r.sort(h||lM)}function f(){for(let d=e,h=n.length;d<h;d++){let g=n[d];if(g.id===null)break;g.id=null,g.object=null,g.geometry=null,g.material=null,g.group=null}}return{opaque:t,transmissive:i,transparent:r,init:s,push:c,unshift:l,finish:f,sort:u}}function oR(){let n=new WeakMap;function e(i,r){let s=n.get(i),o;return s===void 0?(o=new uM,n.set(i,[o])):r>=s.length?(o=new uM,s.push(o)):o=s[r],o}function t(){n=new WeakMap}return{get:e,dispose:t}}function aR(){let n={};return{get:function(e){if(n[e.id]!==void 0)return n[e.id];let t;switch(e.type){case"DirectionalLight":t={direction:new k,color:new Xe};break;case"SpotLight":t={position:new k,direction:new k,color:new Xe,distance:0,coneCos:0,penumbraCos:0,decay:0};break;case"PointLight":t={position:new k,color:new Xe,distance:0,decay:0};break;case"HemisphereLight":t={direction:new k,skyColor:new Xe,groundColor:new Xe};break;case"RectAreaLight":t={color:new Xe,position:new k,halfWidth:new k,halfHeight:new k};break}return n[e.id]=t,t}}}function cR(){let n={};return{get:function(e){if(n[e.id]!==void 0)return n[e.id];let t;switch(e.type){case"DirectionalLight":t={shadowIntensity:1,shadowBias:0,shadowNormalBias:0,shadowRadius:1,shadowMapSize:new ht};break;case"SpotLight":t={shadowIntensity:1,shadowBias:0,shadowNormalBias:0,shadowRadius:1,shadowMapSize:new ht};break;case"PointLight":t={shadowIntensity:1,shadowBias:0,shadowNormalBias:0,shadowRadius:1,shadowMapSize:new ht,shadowCameraNear:1,shadowCameraFar:1e3};break}return n[e.id]=t,t}}}var lR=0;function uR(n,e){return(e.castShadow?2:0)-(n.castShadow?2:0)+(e.map?1:0)-(n.map?1:0)}function dR(n){let e=new aR,t=cR(),i={version:0,hash:{directionalLength:-1,pointLength:-1,spotLength:-1,rectAreaLength:-1,hemiLength:-1,numDirectionalShadows:-1,numPointShadows:-1,numSpotShadows:-1,numSpotMaps:-1,numLightProbes:-1},ambient:[0,0,0],probe:[],directional:[],directionalShadow:[],directionalShadowMap:[],directionalShadowMatrix:[],spot:[],spotLightMap:[],spotShadow:[],spotShadowMap:[],spotLightMatrix:[],rectArea:[],rectAreaLTC1:null,rectAreaLTC2:null,point:[],pointShadow:[],pointShadowMap:[],pointShadowMatrix:[],hemi:[],numSpotLightShadowsWithMaps:0,numLightProbes:0};for(let l=0;l<9;l++)i.probe.push(new k);let r=new k,s=new Ut,o=new Ut;function a(l){let u=0,f=0,d=0;for(let S=0;S<9;S++)i.probe[S].set(0,0,0);let h=0,g=0,x=0,m=0,p=0,M=0,T=0,E=0,A=0,I=0,D=0;l.sort(uR);for(let S=0,q=l.length;S<q;S++){let w=l[S],U=w.color,V=w.intensity,W=w.distance,B=null;if(w.shadow&&w.shadow.map&&(w.shadow.map.texture.format===Ts?B=w.shadow.map.texture:B=w.shadow.map.depthTexture||w.shadow.map.texture),w.isAmbientLight)u+=U.r*V,f+=U.g*V,d+=U.b*V;else if(w.isLightProbe){for(let H=0;H<9;H++)i.probe[H].addScaledVector(w.sh.coefficients[H],V);D++}else if(w.isDirectionalLight){let H=e.get(w);if(H.color.copy(w.color).multiplyScalar(w.intensity),w.castShadow){let L=w.shadow,Q=t.get(w);Q.shadowIntensity=L.intensity,Q.shadowBias=L.bias,Q.shadowNormalBias=L.normalBias,Q.shadowRadius=L.radius,Q.shadowMapSize=L.mapSize,i.directionalShadow[h]=Q,i.directionalShadowMap[h]=B,i.directionalShadowMatrix[h]=w.shadow.matrix,M++}i.directional[h]=H,h++}else if(w.isSpotLight){let H=e.get(w);H.position.setFromMatrixPosition(w.matrixWorld),H.color.copy(U).multiplyScalar(V),H.distance=W,H.coneCos=Math.cos(w.angle),H.penumbraCos=Math.cos(w.angle*(1-w.penumbra)),H.decay=w.decay,i.spot[x]=H;let L=w.shadow;if(w.map&&(i.spotLightMap[A]=w.map,A++,L.updateMatrices(w),w.castShadow&&I++),i.spotLightMatrix[x]=L.matrix,w.castShadow){let Q=t.get(w);Q.shadowIntensity=L.intensity,Q.shadowBias=L.bias,Q.shadowNormalBias=L.normalBias,Q.shadowRadius=L.radius,Q.shadowMapSize=L.mapSize,i.spotShadow[x]=Q,i.spotShadowMap[x]=B,E++}x++}else if(w.isRectAreaLight){let H=e.get(w);H.color.copy(U).multiplyScalar(V),H.halfWidth.set(w.width*.5,0,0),H.halfHeight.set(0,w.height*.5,0),i.rectArea[m]=H,m++}else if(w.isPointLight){let H=e.get(w);if(H.color.copy(w.color).multiplyScalar(w.intensity),H.distance=w.distance,H.decay=w.decay,w.castShadow){let L=w.shadow,Q=t.get(w);Q.shadowIntensity=L.intensity,Q.shadowBias=L.bias,Q.shadowNormalBias=L.normalBias,Q.shadowRadius=L.radius,Q.shadowMapSize=L.mapSize,Q.shadowCameraNear=L.camera.near,Q.shadowCameraFar=L.camera.far,i.pointShadow[g]=Q,i.pointShadowMap[g]=B,i.pointShadowMatrix[g]=w.shadow.matrix,T++}i.point[g]=H,g++}else if(w.isHemisphereLight){let H=e.get(w);H.skyColor.copy(w.color).multiplyScalar(V),H.groundColor.copy(w.groundColor).multiplyScalar(V),i.hemi[p]=H,p++}}m>0&&(n.has("OES_texture_float_linear")===!0?(i.rectAreaLTC1=oe.LTC_FLOAT_1,i.rectAreaLTC2=oe.LTC_FLOAT_2):(i.rectAreaLTC1=oe.LTC_HALF_1,i.rectAreaLTC2=oe.LTC_HALF_2)),i.ambient[0]=u,i.ambient[1]=f,i.ambient[2]=d;let y=i.hash;(y.directionalLength!==h||y.pointLength!==g||y.spotLength!==x||y.rectAreaLength!==m||y.hemiLength!==p||y.numDirectionalShadows!==M||y.numPointShadows!==T||y.numSpotShadows!==E||y.numSpotMaps!==A||y.numLightProbes!==D)&&(i.directional.length=h,i.spot.length=x,i.rectArea.length=m,i.point.length=g,i.hemi.length=p,i.directionalShadow.length=M,i.directionalShadowMap.length=M,i.pointShadow.length=T,i.pointShadowMap.length=T,i.spotShadow.length=E,i.spotShadowMap.length=E,i.directionalShadowMatrix.length=M,i.pointShadowMatrix.length=T,i.spotLightMatrix.length=E+A-I,i.spotLightMap.length=A,i.numSpotLightShadowsWithMaps=I,i.numLightProbes=D,y.directionalLength=h,y.pointLength=g,y.spotLength=x,y.rectAreaLength=m,y.hemiLength=p,y.numDirectionalShadows=M,y.numPointShadows=T,y.numSpotShadows=E,y.numSpotMaps=A,y.numLightProbes=D,i.version=lR++)}function c(l,u){let f=0,d=0,h=0,g=0,x=0,m=u.matrixWorldInverse;for(let p=0,M=l.length;p<M;p++){let T=l[p];if(T.isDirectionalLight){let E=i.directional[f];E.direction.setFromMatrixPosition(T.matrixWorld),r.setFromMatrixPosition(T.target.matrixWorld),E.direction.sub(r),E.direction.transformDirection(m),f++}else if(T.isSpotLight){let E=i.spot[h];E.position.setFromMatrixPosition(T.matrixWorld),E.position.applyMatrix4(m),E.direction.setFromMatrixPosition(T.matrixWorld),r.setFromMatrixPosition(T.target.matrixWorld),E.direction.sub(r),E.direction.transformDirection(m),h++}else if(T.isRectAreaLight){let E=i.rectArea[g];E.position.setFromMatrixPosition(T.matrixWorld),E.position.applyMatrix4(m),o.identity(),s.copy(T.matrixWorld),s.premultiply(m),o.extractRotation(s),E.halfWidth.set(T.width*.5,0,0),E.halfHeight.set(0,T.height*.5,0),E.halfWidth.applyMatrix4(o),E.halfHeight.applyMatrix4(o),g++}else if(T.isPointLight){let E=i.point[d];E.position.setFromMatrixPosition(T.matrixWorld),E.position.applyMatrix4(m),d++}else if(T.isHemisphereLight){let E=i.hemi[x];E.direction.setFromMatrixPosition(T.matrixWorld),E.direction.transformDirection(m),x++}}}return{setup:a,setupView:c,state:i}}function dM(n){let e=new dR(n),t=[],i=[];function r(u){l.camera=u,t.length=0,i.length=0}function s(u){t.push(u)}function o(u){i.push(u)}function a(){e.setup(t)}function c(u){e.setupView(t,u)}let l={lightsArray:t,shadowsArray:i,camera:null,lights:e,transmissionRenderTarget:{}};return{init:r,state:l,setupLights:a,setupLightsView:c,pushLight:s,pushShadow:o}}function fR(n){let e=new WeakMap;function t(r,s=0){let o=e.get(r),a;return o===void 0?(a=new dM(n),e.set(r,[a])):s>=o.length?(a=new dM(n),o.push(a)):a=o[s],a}function i(){e=new WeakMap}return{get:t,dispose:i}}var hR=`void main() {
	gl_Position = vec4( position, 1.0 );
}`,pR=`uniform sampler2D shadow_pass;
uniform vec2 resolution;
uniform float radius;
void main() {
	const float samples = float( VSM_SAMPLES );
	float mean = 0.0;
	float squared_mean = 0.0;
	float uvStride = samples <= 1.0 ? 0.0 : 2.0 / ( samples - 1.0 );
	float uvStart = samples <= 1.0 ? 0.0 : - 1.0;
	for ( float i = 0.0; i < samples; i ++ ) {
		float uvOffset = uvStart + i * uvStride;
		#ifdef HORIZONTAL_PASS
			vec2 distribution = texture2D( shadow_pass, ( gl_FragCoord.xy + vec2( uvOffset, 0.0 ) * radius ) / resolution ).rg;
			mean += distribution.x;
			squared_mean += distribution.y * distribution.y + distribution.x * distribution.x;
		#else
			float depth = texture2D( shadow_pass, ( gl_FragCoord.xy + vec2( 0.0, uvOffset ) * radius ) / resolution ).r;
			mean += depth;
			squared_mean += depth * depth;
		#endif
	}
	mean = mean / samples;
	squared_mean = squared_mean / samples;
	float std_dev = sqrt( max( 0.0, squared_mean - mean * mean ) );
	gl_FragColor = vec4( mean, std_dev, 0.0, 1.0 );
}`,mR=[new k(1,0,0),new k(-1,0,0),new k(0,1,0),new k(0,-1,0),new k(0,0,1),new k(0,0,-1)],gR=[new k(0,-1,0),new k(0,-1,0),new k(0,0,1),new k(0,0,-1),new k(0,-1,0),new k(0,-1,0)],fM=new Ut,rc=new k,gg=new k;function vR(n,e,t){let i=new Va,r=new ht,s=new ht,o=new Pt,a=new Pu,c=new Lu,l={},u=t.maxTextureSize,f={[er]:vn,[vn]:er,[Ii]:Ii},d=new Pn({defines:{VSM_SAMPLES:8},uniforms:{shadow_pass:{value:null},resolution:{value:new ht},radius:{value:4}},vertexShader:hR,fragmentShader:pR}),h=d.clone();h.defines.HORIZONTAL_PASS=1;let g=new Nn;g.setAttribute("position",new hn(new Float32Array([-1,-1,.5,3,-1,.5,-1,3,.5]),3));let x=new Xn(g,d),m=this;this.enabled=!1,this.autoUpdate=!0,this.needsUpdate=!1,this.type=Ya;let p=this.type;this.render=function(I,D,y){if(m.enabled===!1||m.autoUpdate===!1&&m.needsUpdate===!1||I.length===0)return;this.type===c0&&(Ne("WebGLShadowMap: PCFSoftShadowMap has been deprecated. Using PCFShadowMap instead."),this.type=Ya);let S=n.getRenderTarget(),q=n.getActiveCubeFace(),w=n.getActiveMipmapLevel(),U=n.state;U.setBlending(Ai),U.buffers.depth.getReversed()===!0?U.buffers.color.setClear(0,0,0,0):U.buffers.color.setClear(1,1,1,1),U.buffers.depth.setTest(!0),U.setScissorTest(!1);let V=p!==this.type;V&&D.traverse(function(W){W.material&&(Array.isArray(W.material)?W.material.forEach(B=>B.needsUpdate=!0):W.material.needsUpdate=!0)});for(let W=0,B=I.length;W<B;W++){let H=I[W],L=H.shadow;if(L===void 0){Ne("WebGLShadowMap:",H,"has no shadow.");continue}if(L.autoUpdate===!1&&L.needsUpdate===!1)continue;r.copy(L.mapSize);let Q=L.getFrameExtents();r.multiply(Q),s.copy(L.mapSize),(r.x>u||r.y>u)&&(r.x>u&&(s.x=Math.floor(u/Q.x),r.x=s.x*Q.x,L.mapSize.x=s.x),r.y>u&&(s.y=Math.floor(u/Q.y),r.y=s.y*Q.y,L.mapSize.y=s.y));let Z=n.state.buffers.depth.getReversed();if(L.camera._reversedDepth=Z,L.map===null||V===!0){if(L.map!==null&&(L.map.depthTexture!==null&&(L.map.depthTexture.dispose(),L.map.depthTexture=null),L.map.dispose()),this.type===Co){if(H.isPointLight){Ne("WebGLShadowMap: VSM shadow maps are not supported for PointLights. Use PCF or BasicShadowMap instead.");continue}L.map=new Rn(r.x,r.y,{format:Ts,type:Di,minFilter:tn,magFilter:tn,generateMipmaps:!1}),L.map.texture.name=H.name+".shadowMap",L.map.depthTexture=new Dr(r.x,r.y,hi),L.map.depthTexture.name=H.name+".shadowMapDepth",L.map.depthTexture.format=Ti,L.map.depthTexture.compareFunction=null,L.map.depthTexture.minFilter=Kt,L.map.depthTexture.magFilter=Kt}else H.isPointLight?(L.map=new Ud(r.x),L.map.depthTexture=new Ru(r.x,fi)):(L.map=new Rn(r.x,r.y),L.map.depthTexture=new Dr(r.x,r.y,fi)),L.map.depthTexture.name=H.name+".shadowMap",L.map.depthTexture.format=Ti,this.type===Ya?(L.map.depthTexture.compareFunction=Z?Ld:Pd,L.map.depthTexture.minFilter=tn,L.map.depthTexture.magFilter=tn):(L.map.depthTexture.compareFunction=null,L.map.depthTexture.minFilter=Kt,L.map.depthTexture.magFilter=Kt);L.camera.updateProjectionMatrix()}let ue=L.map.isWebGLCubeRenderTarget?6:1;for(let ge=0;ge<ue;ge++){if(L.map.isWebGLCubeRenderTarget)n.setRenderTarget(L.map,ge),n.clear();else{ge===0&&(n.setRenderTarget(L.map),n.clear());let fe=L.getViewport(ge);o.set(s.x*fe.x,s.y*fe.y,s.x*fe.z,s.y*fe.w),U.viewport(o)}if(H.isPointLight){let fe=L.camera,He=L.matrix,Tt=H.distance||fe.far;Tt!==fe.far&&(fe.far=Tt,fe.updateProjectionMatrix()),rc.setFromMatrixPosition(H.matrixWorld),fe.position.copy(rc),gg.copy(fe.position),gg.add(mR[ge]),fe.up.copy(gR[ge]),fe.lookAt(gg),fe.updateMatrixWorld(),He.makeTranslation(-rc.x,-rc.y,-rc.z),fM.multiplyMatrices(fe.projectionMatrix,fe.matrixWorldInverse),L._frustum.setFromProjectionMatrix(fM,fe.coordinateSystem,fe.reversedDepth)}else L.updateMatrices(H);i=L.getFrustum(),E(D,y,L.camera,H,this.type)}L.isPointLightShadow!==!0&&this.type===Co&&M(L,y),L.needsUpdate=!1}p=this.type,m.needsUpdate=!1,n.setRenderTarget(S,q,w)};function M(I,D){let y=e.update(x);d.defines.VSM_SAMPLES!==I.blurSamples&&(d.defines.VSM_SAMPLES=I.blurSamples,h.defines.VSM_SAMPLES=I.blurSamples,d.needsUpdate=!0,h.needsUpdate=!0),I.mapPass===null&&(I.mapPass=new Rn(r.x,r.y,{format:Ts,type:Di})),d.uniforms.shadow_pass.value=I.map.depthTexture,d.uniforms.resolution.value=I.mapSize,d.uniforms.radius.value=I.radius,n.setRenderTarget(I.mapPass),n.clear(),n.renderBufferDirect(D,null,y,d,x,null),h.uniforms.shadow_pass.value=I.mapPass.texture,h.uniforms.resolution.value=I.mapSize,h.uniforms.radius.value=I.radius,n.setRenderTarget(I.map),n.clear(),n.renderBufferDirect(D,null,y,h,x,null)}function T(I,D,y,S){let q=null,w=y.isPointLight===!0?I.customDistanceMaterial:I.customDepthMaterial;if(w!==void 0)q=w;else if(q=y.isPointLight===!0?c:a,n.localClippingEnabled&&D.clipShadows===!0&&Array.isArray(D.clippingPlanes)&&D.clippingPlanes.length!==0||D.displacementMap&&D.displacementScale!==0||D.alphaMap&&D.alphaTest>0||D.map&&D.alphaTest>0||D.alphaToCoverage===!0){let U=q.uuid,V=D.uuid,W=l[U];W===void 0&&(W={},l[U]=W);let B=W[V];B===void 0&&(B=q.clone(),W[V]=B,D.addEventListener("dispose",A)),q=B}if(q.visible=D.visible,q.wireframe=D.wireframe,S===Co?q.side=D.shadowSide!==null?D.shadowSide:D.side:q.side=D.shadowSide!==null?D.shadowSide:f[D.side],q.alphaMap=D.alphaMap,q.alphaTest=D.alphaToCoverage===!0?.5:D.alphaTest,q.map=D.map,q.clipShadows=D.clipShadows,q.clippingPlanes=D.clippingPlanes,q.clipIntersection=D.clipIntersection,q.displacementMap=D.displacementMap,q.displacementScale=D.displacementScale,q.displacementBias=D.displacementBias,q.wireframeLinewidth=D.wireframeLinewidth,q.linewidth=D.linewidth,y.isPointLight===!0&&q.isMeshDistanceMaterial===!0){let U=n.properties.get(q);U.light=y}return q}function E(I,D,y,S,q){if(I.visible===!1)return;if(I.layers.test(D.layers)&&(I.isMesh||I.isLine||I.isPoints)&&(I.castShadow||I.receiveShadow&&q===Co)&&(!I.frustumCulled||i.intersectsObject(I))){I.modelViewMatrix.multiplyMatrices(y.matrixWorldInverse,I.matrixWorld);let V=e.update(I),W=I.material;if(Array.isArray(W)){let B=V.groups;for(let H=0,L=B.length;H<L;H++){let Q=B[H],Z=W[Q.materialIndex];if(Z&&Z.visible){let ue=T(I,Z,S,q);I.onBeforeShadow(n,I,D,y,V,ue,Q),n.renderBufferDirect(y,null,V,ue,I,Q),I.onAfterShadow(n,I,D,y,V,ue,Q)}}}else if(W.visible){let B=T(I,W,S,q);I.onBeforeShadow(n,I,D,y,V,B,null),n.renderBufferDirect(y,null,V,B,I,null),I.onAfterShadow(n,I,D,y,V,B,null)}}let U=I.children;for(let V=0,W=U.length;V<W;V++)E(U[V],D,y,S,q)}function A(I){I.target.removeEventListener("dispose",A);for(let y in l){let S=l[y],q=I.target.uuid;q in S&&(S[q].dispose(),delete S[q])}}}function yR(n,e){function t(){let R=!1,re=new Pt,te=null,pe=new Pt(0,0,0,0);return{setMask:function(K){te!==K&&!R&&(n.colorMask(K,K,K,K),te=K)},setLocked:function(K){R=K},setClear:function(K,G,_e,Fe,Mt){Mt===!0&&(K*=Fe,G*=Fe,_e*=Fe),re.set(K,G,_e,Fe),pe.equals(re)===!1&&(n.clearColor(K,G,_e,Fe),pe.copy(re))},reset:function(){R=!1,te=null,pe.set(-1,0,0,0)}}}function i(){let R=!1,re=!1,te=null,pe=null,K=null;return{setReversed:function(G){if(re!==G){let _e=e.get("EXT_clip_control");G?_e.clipControlEXT(_e.LOWER_LEFT_EXT,_e.ZERO_TO_ONE_EXT):_e.clipControlEXT(_e.LOWER_LEFT_EXT,_e.NEGATIVE_ONE_TO_ONE_EXT),re=G;let Fe=K;K=null,this.setClear(Fe)}},getReversed:function(){return re},setTest:function(G){G?ne(n.DEPTH_TEST):se(n.DEPTH_TEST)},setMask:function(G){te!==G&&!R&&(n.depthMask(G),te=G)},setFunc:function(G){if(re&&(G=z0[G]),pe!==G){switch(G){case gu:n.depthFunc(n.NEVER);break;case vu:n.depthFunc(n.ALWAYS);break;case yu:n.depthFunc(n.LESS);break;case xs:n.depthFunc(n.LEQUAL);break;case _u:n.depthFunc(n.EQUAL);break;case xu:n.depthFunc(n.GEQUAL);break;case Mu:n.depthFunc(n.GREATER);break;case bu:n.depthFunc(n.NOTEQUAL);break;default:n.depthFunc(n.LEQUAL)}pe=G}},setLocked:function(G){R=G},setClear:function(G){K!==G&&(K=G,re&&(G=1-G),n.clearDepth(G))},reset:function(){R=!1,te=null,pe=null,K=null,re=!1}}}function r(){let R=!1,re=null,te=null,pe=null,K=null,G=null,_e=null,Fe=null,Mt=null;return{setTest:function(ft){R||(ft?ne(n.STENCIL_TEST):se(n.STENCIL_TEST))},setMask:function(ft){re!==ft&&!R&&(n.stencilMask(ft),re=ft)},setFunc:function(ft,Pi,Li){(te!==ft||pe!==Pi||K!==Li)&&(n.stencilFunc(ft,Pi,Li),te=ft,pe=Pi,K=Li)},setOp:function(ft,Pi,Li){(G!==ft||_e!==Pi||Fe!==Li)&&(n.stencilOp(ft,Pi,Li),G=ft,_e=Pi,Fe=Li)},setLocked:function(ft){R=ft},setClear:function(ft){Mt!==ft&&(n.clearStencil(ft),Mt=ft)},reset:function(){R=!1,re=null,te=null,pe=null,K=null,G=null,_e=null,Fe=null,Mt=null}}}let s=new t,o=new i,a=new r,c=new WeakMap,l=new WeakMap,u={},f={},d=new WeakMap,h=[],g=null,x=!1,m=null,p=null,M=null,T=null,E=null,A=null,I=null,D=new Xe(0,0,0),y=0,S=!1,q=null,w=null,U=null,V=null,W=null,B=n.getParameter(n.MAX_COMBINED_TEXTURE_IMAGE_UNITS),H=!1,L=0,Q=n.getParameter(n.VERSION);Q.indexOf("WebGL")!==-1?(L=parseFloat(/^WebGL (\d)/.exec(Q)[1]),H=L>=1):Q.indexOf("OpenGL ES")!==-1&&(L=parseFloat(/^OpenGL ES (\d)/.exec(Q)[1]),H=L>=2);let Z=null,ue={},ge=n.getParameter(n.SCISSOR_BOX),fe=n.getParameter(n.VIEWPORT),He=new Pt().fromArray(ge),Tt=new Pt().fromArray(fe);function St(R,re,te,pe){let K=new Uint8Array(4),G=n.createTexture();n.bindTexture(R,G),n.texParameteri(R,n.TEXTURE_MIN_FILTER,n.NEAREST),n.texParameteri(R,n.TEXTURE_MAG_FILTER,n.NEAREST);for(let _e=0;_e<te;_e++)R===n.TEXTURE_3D||R===n.TEXTURE_2D_ARRAY?n.texImage3D(re,0,n.RGBA,1,1,pe,0,n.RGBA,n.UNSIGNED_BYTE,K):n.texImage2D(re+_e,0,n.RGBA,1,1,0,n.RGBA,n.UNSIGNED_BYTE,K);return G}let X={};X[n.TEXTURE_2D]=St(n.TEXTURE_2D,n.TEXTURE_2D,1),X[n.TEXTURE_CUBE_MAP]=St(n.TEXTURE_CUBE_MAP,n.TEXTURE_CUBE_MAP_POSITIVE_X,6),X[n.TEXTURE_2D_ARRAY]=St(n.TEXTURE_2D_ARRAY,n.TEXTURE_2D_ARRAY,1,1),X[n.TEXTURE_3D]=St(n.TEXTURE_3D,n.TEXTURE_3D,1,1),s.setClear(0,0,0,1),o.setClear(1),a.setClear(0),ne(n.DEPTH_TEST),o.setFunc(xs),Ge(!1),Lt(Vm),ne(n.CULL_FACE),dt(Ai);function ne(R){u[R]!==!0&&(n.enable(R),u[R]=!0)}function se(R){u[R]!==!1&&(n.disable(R),u[R]=!1)}function Ve(R,re){return f[R]!==re?(n.bindFramebuffer(R,re),f[R]=re,R===n.DRAW_FRAMEBUFFER&&(f[n.FRAMEBUFFER]=re),R===n.FRAMEBUFFER&&(f[n.DRAW_FRAMEBUFFER]=re),!0):!1}function Ie(R,re){let te=h,pe=!1;if(R){te=d.get(re),te===void 0&&(te=[],d.set(re,te));let K=R.textures;if(te.length!==K.length||te[0]!==n.COLOR_ATTACHMENT0){for(let G=0,_e=K.length;G<_e;G++)te[G]=n.COLOR_ATTACHMENT0+G;te.length=K.length,pe=!0}}else te[0]!==n.BACK&&(te[0]=n.BACK,pe=!0);pe&&n.drawBuffers(te)}function Pe(R){return g!==R?(n.useProgram(R),g=R,!0):!1}let Yt={[Ir]:n.FUNC_ADD,[u0]:n.FUNC_SUBTRACT,[d0]:n.FUNC_REVERSE_SUBTRACT};Yt[f0]=n.MIN,Yt[h0]=n.MAX;let nt={[p0]:n.ZERO,[m0]:n.ONE,[g0]:n.SRC_COLOR,[pu]:n.SRC_ALPHA,[b0]:n.SRC_ALPHA_SATURATE,[x0]:n.DST_COLOR,[y0]:n.DST_ALPHA,[v0]:n.ONE_MINUS_SRC_COLOR,[mu]:n.ONE_MINUS_SRC_ALPHA,[M0]:n.ONE_MINUS_DST_COLOR,[_0]:n.ONE_MINUS_DST_ALPHA,[S0]:n.CONSTANT_COLOR,[E0]:n.ONE_MINUS_CONSTANT_COLOR,[T0]:n.CONSTANT_ALPHA,[w0]:n.ONE_MINUS_CONSTANT_ALPHA};function dt(R,re,te,pe,K,G,_e,Fe,Mt,ft){if(R===Ai){x===!0&&(se(n.BLEND),x=!1);return}if(x===!1&&(ne(n.BLEND),x=!0),R!==l0){if(R!==m||ft!==S){if((p!==Ir||E!==Ir)&&(n.blendEquation(n.FUNC_ADD),p=Ir,E=Ir),ft)switch(R){case tr:n.blendFuncSeparate(n.ONE,n.ONE_MINUS_SRC_ALPHA,n.ONE,n.ONE_MINUS_SRC_ALPHA);break;case Hm:n.blendFunc(n.ONE,n.ONE);break;case zm:n.blendFuncSeparate(n.ZERO,n.ONE_MINUS_SRC_COLOR,n.ZERO,n.ONE);break;case Gm:n.blendFuncSeparate(n.DST_COLOR,n.ONE_MINUS_SRC_ALPHA,n.ZERO,n.ONE);break;default:Re("WebGLState: Invalid blending: ",R);break}else switch(R){case tr:n.blendFuncSeparate(n.SRC_ALPHA,n.ONE_MINUS_SRC_ALPHA,n.ONE,n.ONE_MINUS_SRC_ALPHA);break;case Hm:n.blendFuncSeparate(n.SRC_ALPHA,n.ONE,n.ONE,n.ONE);break;case zm:Re("WebGLState: SubtractiveBlending requires material.premultipliedAlpha = true");break;case Gm:Re("WebGLState: MultiplyBlending requires material.premultipliedAlpha = true");break;default:Re("WebGLState: Invalid blending: ",R);break}M=null,T=null,A=null,I=null,D.set(0,0,0),y=0,m=R,S=ft}return}K=K||re,G=G||te,_e=_e||pe,(re!==p||K!==E)&&(n.blendEquationSeparate(Yt[re],Yt[K]),p=re,E=K),(te!==M||pe!==T||G!==A||_e!==I)&&(n.blendFuncSeparate(nt[te],nt[pe],nt[G],nt[_e]),M=te,T=pe,A=G,I=_e),(Fe.equals(D)===!1||Mt!==y)&&(n.blendColor(Fe.r,Fe.g,Fe.b,Mt),D.copy(Fe),y=Mt),m=R,S=!1}function yt(R,re){R.side===Ii?se(n.CULL_FACE):ne(n.CULL_FACE);let te=R.side===vn;re&&(te=!te),Ge(te),R.blending===tr&&R.transparent===!1?dt(Ai):dt(R.blending,R.blendEquation,R.blendSrc,R.blendDst,R.blendEquationAlpha,R.blendSrcAlpha,R.blendDstAlpha,R.blendColor,R.blendAlpha,R.premultipliedAlpha),o.setFunc(R.depthFunc),o.setTest(R.depthTest),o.setMask(R.depthWrite),s.setMask(R.colorWrite);let pe=R.stencilWrite;a.setTest(pe),pe&&(a.setMask(R.stencilWriteMask),a.setFunc(R.stencilFunc,R.stencilRef,R.stencilFuncMask),a.setOp(R.stencilFail,R.stencilZFail,R.stencilZPass)),Bt(R.polygonOffset,R.polygonOffsetFactor,R.polygonOffsetUnits),R.alphaToCoverage===!0?ne(n.SAMPLE_ALPHA_TO_COVERAGE):se(n.SAMPLE_ALPHA_TO_COVERAGE)}function Ge(R){q!==R&&(R?n.frontFace(n.CW):n.frontFace(n.CCW),q=R)}function Lt(R){R!==o0?(ne(n.CULL_FACE),R!==w&&(R===Vm?n.cullFace(n.BACK):R===a0?n.cullFace(n.FRONT):n.cullFace(n.FRONT_AND_BACK))):se(n.CULL_FACE),w=R}function C(R){R!==U&&(H&&n.lineWidth(R),U=R)}function Bt(R,re,te){R?(ne(n.POLYGON_OFFSET_FILL),(V!==re||W!==te)&&(V=re,W=te,o.getReversed()&&(re=-re),n.polygonOffset(re,te))):se(n.POLYGON_OFFSET_FILL)}function lt(R){R?ne(n.SCISSOR_TEST):se(n.SCISSOR_TEST)}function xt(R){R===void 0&&(R=n.TEXTURE0+B-1),Z!==R&&(n.activeTexture(R),Z=R)}function be(R,re,te){te===void 0&&(Z===null?te=n.TEXTURE0+B-1:te=Z);let pe=ue[te];pe===void 0&&(pe={type:void 0,texture:void 0},ue[te]=pe),(pe.type!==R||pe.texture!==re)&&(Z!==te&&(n.activeTexture(te),Z=te),n.bindTexture(R,re||X[R]),pe.type=R,pe.texture=re)}function b(){let R=ue[Z];R!==void 0&&R.type!==void 0&&(n.bindTexture(R.type,null),R.type=void 0,R.texture=void 0)}function v(){try{n.compressedTexImage2D(...arguments)}catch(R){Re("WebGLState:",R)}}function N(){try{n.compressedTexImage3D(...arguments)}catch(R){Re("WebGLState:",R)}}function $(){try{n.texSubImage2D(...arguments)}catch(R){Re("WebGLState:",R)}}function Y(){try{n.texSubImage3D(...arguments)}catch(R){Re("WebGLState:",R)}}function j(){try{n.compressedTexSubImage2D(...arguments)}catch(R){Re("WebGLState:",R)}}function ve(){try{n.compressedTexSubImage3D(...arguments)}catch(R){Re("WebGLState:",R)}}function ie(){try{n.texStorage2D(...arguments)}catch(R){Re("WebGLState:",R)}}function we(){try{n.texStorage3D(...arguments)}catch(R){Re("WebGLState:",R)}}function De(){try{n.texImage2D(...arguments)}catch(R){Re("WebGLState:",R)}}function J(){try{n.texImage3D(...arguments)}catch(R){Re("WebGLState:",R)}}function ee(R){He.equals(R)===!1&&(n.scissor(R.x,R.y,R.z,R.w),He.copy(R))}function ye(R){Tt.equals(R)===!1&&(n.viewport(R.x,R.y,R.z,R.w),Tt.copy(R))}function xe(R,re){let te=l.get(re);te===void 0&&(te=new WeakMap,l.set(re,te));let pe=te.get(R);pe===void 0&&(pe=n.getUniformBlockIndex(re,R.name),te.set(R,pe))}function de(R,re){let pe=l.get(re).get(R);c.get(re)!==pe&&(n.uniformBlockBinding(re,pe,R.__bindingPointIndex),c.set(re,pe))}function We(){n.disable(n.BLEND),n.disable(n.CULL_FACE),n.disable(n.DEPTH_TEST),n.disable(n.POLYGON_OFFSET_FILL),n.disable(n.SCISSOR_TEST),n.disable(n.STENCIL_TEST),n.disable(n.SAMPLE_ALPHA_TO_COVERAGE),n.blendEquation(n.FUNC_ADD),n.blendFunc(n.ONE,n.ZERO),n.blendFuncSeparate(n.ONE,n.ZERO,n.ONE,n.ZERO),n.blendColor(0,0,0,0),n.colorMask(!0,!0,!0,!0),n.clearColor(0,0,0,0),n.depthMask(!0),n.depthFunc(n.LESS),o.setReversed(!1),n.clearDepth(1),n.stencilMask(4294967295),n.stencilFunc(n.ALWAYS,0,4294967295),n.stencilOp(n.KEEP,n.KEEP,n.KEEP),n.clearStencil(0),n.cullFace(n.BACK),n.frontFace(n.CCW),n.polygonOffset(0,0),n.activeTexture(n.TEXTURE0),n.bindFramebuffer(n.FRAMEBUFFER,null),n.bindFramebuffer(n.DRAW_FRAMEBUFFER,null),n.bindFramebuffer(n.READ_FRAMEBUFFER,null),n.useProgram(null),n.lineWidth(1),n.scissor(0,0,n.canvas.width,n.canvas.height),n.viewport(0,0,n.canvas.width,n.canvas.height),u={},Z=null,ue={},f={},d=new WeakMap,h=[],g=null,x=!1,m=null,p=null,M=null,T=null,E=null,A=null,I=null,D=new Xe(0,0,0),y=0,S=!1,q=null,w=null,U=null,V=null,W=null,He.set(0,0,n.canvas.width,n.canvas.height),Tt.set(0,0,n.canvas.width,n.canvas.height),s.reset(),o.reset(),a.reset()}return{buffers:{color:s,depth:o,stencil:a},enable:ne,disable:se,bindFramebuffer:Ve,drawBuffers:Ie,useProgram:Pe,setBlending:dt,setMaterial:yt,setFlipSided:Ge,setCullFace:Lt,setLineWidth:C,setPolygonOffset:Bt,setScissorTest:lt,activeTexture:xt,bindTexture:be,unbindTexture:b,compressedTexImage2D:v,compressedTexImage3D:N,texImage2D:De,texImage3D:J,updateUBOMapping:xe,uniformBlockBinding:de,texStorage2D:ie,texStorage3D:we,texSubImage2D:$,texSubImage3D:Y,compressedTexSubImage2D:j,compressedTexSubImage3D:ve,scissor:ee,viewport:ye,reset:We}}function _R(n,e,t,i,r,s,o){let a=e.has("WEBGL_multisampled_render_to_texture")?e.get("WEBGL_multisampled_render_to_texture"):null,c=typeof navigator>"u"?!1:/OculusBrowser/g.test(navigator.userAgent),l=new ht,u=new WeakMap,f,d=new WeakMap,h=!1;try{h=typeof OffscreenCanvas<"u"&&new OffscreenCanvas(1,1).getContext("2d")!==null}catch{}function g(b,v){return h?new OffscreenCanvas(b,v):Ra("canvas")}function x(b,v,N){let $=1,Y=be(b);if((Y.width>N||Y.height>N)&&($=N/Math.max(Y.width,Y.height)),$<1)if(typeof HTMLImageElement<"u"&&b instanceof HTMLImageElement||typeof HTMLCanvasElement<"u"&&b instanceof HTMLCanvasElement||typeof ImageBitmap<"u"&&b instanceof ImageBitmap||typeof VideoFrame<"u"&&b instanceof VideoFrame){let j=Math.floor($*Y.width),ve=Math.floor($*Y.height);f===void 0&&(f=g(j,ve));let ie=v?g(j,ve):f;return ie.width=j,ie.height=ve,ie.getContext("2d").drawImage(b,0,0,j,ve),Ne("WebGLRenderer: Texture has been resized from ("+Y.width+"x"+Y.height+") to ("+j+"x"+ve+")."),ie}else return"data"in b&&Ne("WebGLRenderer: Image in DataTexture is too big ("+Y.width+"x"+Y.height+")."),b;return b}function m(b){return b.generateMipmaps}function p(b){n.generateMipmap(b)}function M(b){return b.isWebGLCubeRenderTarget?n.TEXTURE_CUBE_MAP:b.isWebGL3DRenderTarget?n.TEXTURE_3D:b.isWebGLArrayRenderTarget||b.isCompressedArrayTexture?n.TEXTURE_2D_ARRAY:n.TEXTURE_2D}function T(b,v,N,$,Y=!1){if(b!==null){if(n[b]!==void 0)return n[b];Ne("WebGLRenderer: Attempt to use non-existing WebGL internal format '"+b+"'")}let j=v;if(v===n.RED&&(N===n.FLOAT&&(j=n.R32F),N===n.HALF_FLOAT&&(j=n.R16F),N===n.UNSIGNED_BYTE&&(j=n.R8)),v===n.RED_INTEGER&&(N===n.UNSIGNED_BYTE&&(j=n.R8UI),N===n.UNSIGNED_SHORT&&(j=n.R16UI),N===n.UNSIGNED_INT&&(j=n.R32UI),N===n.BYTE&&(j=n.R8I),N===n.SHORT&&(j=n.R16I),N===n.INT&&(j=n.R32I)),v===n.RG&&(N===n.FLOAT&&(j=n.RG32F),N===n.HALF_FLOAT&&(j=n.RG16F),N===n.UNSIGNED_BYTE&&(j=n.RG8)),v===n.RG_INTEGER&&(N===n.UNSIGNED_BYTE&&(j=n.RG8UI),N===n.UNSIGNED_SHORT&&(j=n.RG16UI),N===n.UNSIGNED_INT&&(j=n.RG32UI),N===n.BYTE&&(j=n.RG8I),N===n.SHORT&&(j=n.RG16I),N===n.INT&&(j=n.RG32I)),v===n.RGB_INTEGER&&(N===n.UNSIGNED_BYTE&&(j=n.RGB8UI),N===n.UNSIGNED_SHORT&&(j=n.RGB16UI),N===n.UNSIGNED_INT&&(j=n.RGB32UI),N===n.BYTE&&(j=n.RGB8I),N===n.SHORT&&(j=n.RGB16I),N===n.INT&&(j=n.RGB32I)),v===n.RGBA_INTEGER&&(N===n.UNSIGNED_BYTE&&(j=n.RGBA8UI),N===n.UNSIGNED_SHORT&&(j=n.RGBA16UI),N===n.UNSIGNED_INT&&(j=n.RGBA32UI),N===n.BYTE&&(j=n.RGBA8I),N===n.SHORT&&(j=n.RGBA16I),N===n.INT&&(j=n.RGBA32I)),v===n.RGB&&(N===n.UNSIGNED_INT_5_9_9_9_REV&&(j=n.RGB9_E5),N===n.UNSIGNED_INT_10F_11F_11F_REV&&(j=n.R11F_G11F_B10F)),v===n.RGBA){let ve=Y?Aa:tt.getTransfer($);N===n.FLOAT&&(j=n.RGBA32F),N===n.HALF_FLOAT&&(j=n.RGBA16F),N===n.UNSIGNED_BYTE&&(j=ve===ut?n.SRGB8_ALPHA8:n.RGBA8),N===n.UNSIGNED_SHORT_4_4_4_4&&(j=n.RGBA4),N===n.UNSIGNED_SHORT_5_5_5_1&&(j=n.RGB5_A1)}return(j===n.R16F||j===n.R32F||j===n.RG16F||j===n.RG32F||j===n.RGBA16F||j===n.RGBA32F)&&e.get("EXT_color_buffer_float"),j}function E(b,v){let N;return b?v===null||v===fi||v===Ao?N=n.DEPTH24_STENCIL8:v===hi?N=n.DEPTH32F_STENCIL8:v===Io&&(N=n.DEPTH24_STENCIL8,Ne("DepthTexture: 16 bit depth attachment is not supported with stencil. Using 24-bit attachment.")):v===null||v===fi||v===Ao?N=n.DEPTH_COMPONENT24:v===hi?N=n.DEPTH_COMPONENT32F:v===Io&&(N=n.DEPTH_COMPONENT16),N}function A(b,v){return m(b)===!0||b.isFramebufferTexture&&b.minFilter!==Kt&&b.minFilter!==tn?Math.log2(Math.max(v.width,v.height))+1:b.mipmaps!==void 0&&b.mipmaps.length>0?b.mipmaps.length:b.isCompressedTexture&&Array.isArray(b.image)?v.mipmaps.length:1}function I(b){let v=b.target;v.removeEventListener("dispose",I),y(v),v.isVideoTexture&&u.delete(v)}function D(b){let v=b.target;v.removeEventListener("dispose",D),q(v)}function y(b){let v=i.get(b);if(v.__webglInit===void 0)return;let N=b.source,$=d.get(N);if($){let Y=$[v.__cacheKey];Y.usedTimes--,Y.usedTimes===0&&S(b),Object.keys($).length===0&&d.delete(N)}i.remove(b)}function S(b){let v=i.get(b);n.deleteTexture(v.__webglTexture);let N=b.source,$=d.get(N);delete $[v.__cacheKey],o.memory.textures--}function q(b){let v=i.get(b);if(b.depthTexture&&(b.depthTexture.dispose(),i.remove(b.depthTexture)),b.isWebGLCubeRenderTarget)for(let $=0;$<6;$++){if(Array.isArray(v.__webglFramebuffer[$]))for(let Y=0;Y<v.__webglFramebuffer[$].length;Y++)n.deleteFramebuffer(v.__webglFramebuffer[$][Y]);else n.deleteFramebuffer(v.__webglFramebuffer[$]);v.__webglDepthbuffer&&n.deleteRenderbuffer(v.__webglDepthbuffer[$])}else{if(Array.isArray(v.__webglFramebuffer))for(let $=0;$<v.__webglFramebuffer.length;$++)n.deleteFramebuffer(v.__webglFramebuffer[$]);else n.deleteFramebuffer(v.__webglFramebuffer);if(v.__webglDepthbuffer&&n.deleteRenderbuffer(v.__webglDepthbuffer),v.__webglMultisampledFramebuffer&&n.deleteFramebuffer(v.__webglMultisampledFramebuffer),v.__webglColorRenderbuffer)for(let $=0;$<v.__webglColorRenderbuffer.length;$++)v.__webglColorRenderbuffer[$]&&n.deleteRenderbuffer(v.__webglColorRenderbuffer[$]);v.__webglDepthRenderbuffer&&n.deleteRenderbuffer(v.__webglDepthRenderbuffer)}let N=b.textures;for(let $=0,Y=N.length;$<Y;$++){let j=i.get(N[$]);j.__webglTexture&&(n.deleteTexture(j.__webglTexture),o.memory.textures--),i.remove(N[$])}i.remove(b)}let w=0;function U(){w=0}function V(){let b=w;return b>=r.maxTextures&&Ne("WebGLTextures: Trying to use "+b+" texture units while this GPU supports only "+r.maxTextures),w+=1,b}function W(b){let v=[];return v.push(b.wrapS),v.push(b.wrapT),v.push(b.wrapR||0),v.push(b.magFilter),v.push(b.minFilter),v.push(b.anisotropy),v.push(b.internalFormat),v.push(b.format),v.push(b.type),v.push(b.generateMipmaps),v.push(b.premultiplyAlpha),v.push(b.flipY),v.push(b.unpackAlignment),v.push(b.colorSpace),v.join()}function B(b,v){let N=i.get(b);if(b.isVideoTexture&&lt(b),b.isRenderTargetTexture===!1&&b.isExternalTexture!==!0&&b.version>0&&N.__version!==b.version){let $=b.image;if($===null)Ne("WebGLRenderer: Texture marked for update but no image data found.");else if($.complete===!1)Ne("WebGLRenderer: Texture marked for update but image is incomplete");else{X(N,b,v);return}}else b.isExternalTexture&&(N.__webglTexture=b.sourceTexture?b.sourceTexture:null);t.bindTexture(n.TEXTURE_2D,N.__webglTexture,n.TEXTURE0+v)}function H(b,v){let N=i.get(b);if(b.isRenderTargetTexture===!1&&b.version>0&&N.__version!==b.version){X(N,b,v);return}else b.isExternalTexture&&(N.__webglTexture=b.sourceTexture?b.sourceTexture:null);t.bindTexture(n.TEXTURE_2D_ARRAY,N.__webglTexture,n.TEXTURE0+v)}function L(b,v){let N=i.get(b);if(b.isRenderTargetTexture===!1&&b.version>0&&N.__version!==b.version){X(N,b,v);return}t.bindTexture(n.TEXTURE_3D,N.__webglTexture,n.TEXTURE0+v)}function Q(b,v){let N=i.get(b);if(b.isCubeDepthTexture!==!0&&b.version>0&&N.__version!==b.version){ne(N,b,v);return}t.bindTexture(n.TEXTURE_CUBE_MAP,N.__webglTexture,n.TEXTURE0+v)}let Z={[Su]:n.REPEAT,[Ei]:n.CLAMP_TO_EDGE,[Eu]:n.MIRRORED_REPEAT},ue={[Kt]:n.NEAREST,[A0]:n.NEAREST_MIPMAP_NEAREST,[Ja]:n.NEAREST_MIPMAP_LINEAR,[tn]:n.LINEAR,[Xu]:n.LINEAR_MIPMAP_NEAREST,[Or]:n.LINEAR_MIPMAP_LINEAR},ge={[P0]:n.NEVER,[U0]:n.ALWAYS,[L0]:n.LESS,[Pd]:n.LEQUAL,[O0]:n.EQUAL,[Ld]:n.GEQUAL,[F0]:n.GREATER,[k0]:n.NOTEQUAL};function fe(b,v){if(v.type===hi&&e.has("OES_texture_float_linear")===!1&&(v.magFilter===tn||v.magFilter===Xu||v.magFilter===Ja||v.magFilter===Or||v.minFilter===tn||v.minFilter===Xu||v.minFilter===Ja||v.minFilter===Or)&&Ne("WebGLRenderer: Unable to use linear filtering with floating point textures. OES_texture_float_linear not supported on this device."),n.texParameteri(b,n.TEXTURE_WRAP_S,Z[v.wrapS]),n.texParameteri(b,n.TEXTURE_WRAP_T,Z[v.wrapT]),(b===n.TEXTURE_3D||b===n.TEXTURE_2D_ARRAY)&&n.texParameteri(b,n.TEXTURE_WRAP_R,Z[v.wrapR]),n.texParameteri(b,n.TEXTURE_MAG_FILTER,ue[v.magFilter]),n.texParameteri(b,n.TEXTURE_MIN_FILTER,ue[v.minFilter]),v.compareFunction&&(n.texParameteri(b,n.TEXTURE_COMPARE_MODE,n.COMPARE_REF_TO_TEXTURE),n.texParameteri(b,n.TEXTURE_COMPARE_FUNC,ge[v.compareFunction])),e.has("EXT_texture_filter_anisotropic")===!0){if(v.magFilter===Kt||v.minFilter!==Ja&&v.minFilter!==Or||v.type===hi&&e.has("OES_texture_float_linear")===!1)return;if(v.anisotropy>1||i.get(v).__currentAnisotropy){let N=e.get("EXT_texture_filter_anisotropic");n.texParameterf(b,N.TEXTURE_MAX_ANISOTROPY_EXT,Math.min(v.anisotropy,r.getMaxAnisotropy())),i.get(v).__currentAnisotropy=v.anisotropy}}}function He(b,v){let N=!1;b.__webglInit===void 0&&(b.__webglInit=!0,v.addEventListener("dispose",I));let $=v.source,Y=d.get($);Y===void 0&&(Y={},d.set($,Y));let j=W(v);if(j!==b.__cacheKey){Y[j]===void 0&&(Y[j]={texture:n.createTexture(),usedTimes:0},o.memory.textures++,N=!0),Y[j].usedTimes++;let ve=Y[b.__cacheKey];ve!==void 0&&(Y[b.__cacheKey].usedTimes--,ve.usedTimes===0&&S(v)),b.__cacheKey=j,b.__webglTexture=Y[j].texture}return N}function Tt(b,v,N){return Math.floor(Math.floor(b/N)/v)}function St(b,v,N,$){let j=b.updateRanges;if(j.length===0)t.texSubImage2D(n.TEXTURE_2D,0,0,0,v.width,v.height,N,$,v.data);else{j.sort((J,ee)=>J.start-ee.start);let ve=0;for(let J=1;J<j.length;J++){let ee=j[ve],ye=j[J],xe=ee.start+ee.count,de=Tt(ye.start,v.width,4),We=Tt(ee.start,v.width,4);ye.start<=xe+1&&de===We&&Tt(ye.start+ye.count-1,v.width,4)===de?ee.count=Math.max(ee.count,ye.start+ye.count-ee.start):(++ve,j[ve]=ye)}j.length=ve+1;let ie=n.getParameter(n.UNPACK_ROW_LENGTH),we=n.getParameter(n.UNPACK_SKIP_PIXELS),De=n.getParameter(n.UNPACK_SKIP_ROWS);n.pixelStorei(n.UNPACK_ROW_LENGTH,v.width);for(let J=0,ee=j.length;J<ee;J++){let ye=j[J],xe=Math.floor(ye.start/4),de=Math.ceil(ye.count/4),We=xe%v.width,R=Math.floor(xe/v.width),re=de,te=1;n.pixelStorei(n.UNPACK_SKIP_PIXELS,We),n.pixelStorei(n.UNPACK_SKIP_ROWS,R),t.texSubImage2D(n.TEXTURE_2D,0,We,R,re,te,N,$,v.data)}b.clearUpdateRanges(),n.pixelStorei(n.UNPACK_ROW_LENGTH,ie),n.pixelStorei(n.UNPACK_SKIP_PIXELS,we),n.pixelStorei(n.UNPACK_SKIP_ROWS,De)}}function X(b,v,N){let $=n.TEXTURE_2D;(v.isDataArrayTexture||v.isCompressedArrayTexture)&&($=n.TEXTURE_2D_ARRAY),v.isData3DTexture&&($=n.TEXTURE_3D);let Y=He(b,v),j=v.source;t.bindTexture($,b.__webglTexture,n.TEXTURE0+N);let ve=i.get(j);if(j.version!==ve.__version||Y===!0){t.activeTexture(n.TEXTURE0+N);let ie=tt.getPrimaries(tt.workingColorSpace),we=v.colorSpace===ir?null:tt.getPrimaries(v.colorSpace),De=v.colorSpace===ir||ie===we?n.NONE:n.BROWSER_DEFAULT_WEBGL;n.pixelStorei(n.UNPACK_FLIP_Y_WEBGL,v.flipY),n.pixelStorei(n.UNPACK_PREMULTIPLY_ALPHA_WEBGL,v.premultiplyAlpha),n.pixelStorei(n.UNPACK_ALIGNMENT,v.unpackAlignment),n.pixelStorei(n.UNPACK_COLORSPACE_CONVERSION_WEBGL,De);let J=x(v.image,!1,r.maxTextureSize);J=xt(v,J);let ee=s.convert(v.format,v.colorSpace),ye=s.convert(v.type),xe=T(v.internalFormat,ee,ye,v.colorSpace,v.isVideoTexture);fe($,v);let de,We=v.mipmaps,R=v.isVideoTexture!==!0,re=ve.__version===void 0||Y===!0,te=j.dataReady,pe=A(v,J);if(v.isDepthTexture)xe=E(v.format===Fr,v.type),re&&(R?t.texStorage2D(n.TEXTURE_2D,1,xe,J.width,J.height):t.texImage2D(n.TEXTURE_2D,0,xe,J.width,J.height,0,ee,ye,null));else if(v.isDataTexture)if(We.length>0){R&&re&&t.texStorage2D(n.TEXTURE_2D,pe,xe,We[0].width,We[0].height);for(let K=0,G=We.length;K<G;K++)de=We[K],R?te&&t.texSubImage2D(n.TEXTURE_2D,K,0,0,de.width,de.height,ee,ye,de.data):t.texImage2D(n.TEXTURE_2D,K,xe,de.width,de.height,0,ee,ye,de.data);v.generateMipmaps=!1}else R?(re&&t.texStorage2D(n.TEXTURE_2D,pe,xe,J.width,J.height),te&&St(v,J,ee,ye)):t.texImage2D(n.TEXTURE_2D,0,xe,J.width,J.height,0,ee,ye,J.data);else if(v.isCompressedTexture)if(v.isCompressedArrayTexture){R&&re&&t.texStorage3D(n.TEXTURE_2D_ARRAY,pe,xe,We[0].width,We[0].height,J.depth);for(let K=0,G=We.length;K<G;K++)if(de=We[K],v.format!==Yn)if(ee!==null)if(R){if(te)if(v.layerUpdates.size>0){let _e=ug(de.width,de.height,v.format,v.type);for(let Fe of v.layerUpdates){let Mt=de.data.subarray(Fe*_e/de.data.BYTES_PER_ELEMENT,(Fe+1)*_e/de.data.BYTES_PER_ELEMENT);t.compressedTexSubImage3D(n.TEXTURE_2D_ARRAY,K,0,0,Fe,de.width,de.height,1,ee,Mt)}v.clearLayerUpdates()}else t.compressedTexSubImage3D(n.TEXTURE_2D_ARRAY,K,0,0,0,de.width,de.height,J.depth,ee,de.data)}else t.compressedTexImage3D(n.TEXTURE_2D_ARRAY,K,xe,de.width,de.height,J.depth,0,de.data,0,0);else Ne("WebGLRenderer: Attempt to load unsupported compressed texture format in .uploadTexture()");else R?te&&t.texSubImage3D(n.TEXTURE_2D_ARRAY,K,0,0,0,de.width,de.height,J.depth,ee,ye,de.data):t.texImage3D(n.TEXTURE_2D_ARRAY,K,xe,de.width,de.height,J.depth,0,ee,ye,de.data)}else{R&&re&&t.texStorage2D(n.TEXTURE_2D,pe,xe,We[0].width,We[0].height);for(let K=0,G=We.length;K<G;K++)de=We[K],v.format!==Yn?ee!==null?R?te&&t.compressedTexSubImage2D(n.TEXTURE_2D,K,0,0,de.width,de.height,ee,de.data):t.compressedTexImage2D(n.TEXTURE_2D,K,xe,de.width,de.height,0,de.data):Ne("WebGLRenderer: Attempt to load unsupported compressed texture format in .uploadTexture()"):R?te&&t.texSubImage2D(n.TEXTURE_2D,K,0,0,de.width,de.height,ee,ye,de.data):t.texImage2D(n.TEXTURE_2D,K,xe,de.width,de.height,0,ee,ye,de.data)}else if(v.isDataArrayTexture)if(R){if(re&&t.texStorage3D(n.TEXTURE_2D_ARRAY,pe,xe,J.width,J.height,J.depth),te)if(v.layerUpdates.size>0){let K=ug(J.width,J.height,v.format,v.type);for(let G of v.layerUpdates){let _e=J.data.subarray(G*K/J.data.BYTES_PER_ELEMENT,(G+1)*K/J.data.BYTES_PER_ELEMENT);t.texSubImage3D(n.TEXTURE_2D_ARRAY,0,0,0,G,J.width,J.height,1,ee,ye,_e)}v.clearLayerUpdates()}else t.texSubImage3D(n.TEXTURE_2D_ARRAY,0,0,0,0,J.width,J.height,J.depth,ee,ye,J.data)}else t.texImage3D(n.TEXTURE_2D_ARRAY,0,xe,J.width,J.height,J.depth,0,ee,ye,J.data);else if(v.isData3DTexture)R?(re&&t.texStorage3D(n.TEXTURE_3D,pe,xe,J.width,J.height,J.depth),te&&t.texSubImage3D(n.TEXTURE_3D,0,0,0,0,J.width,J.height,J.depth,ee,ye,J.data)):t.texImage3D(n.TEXTURE_3D,0,xe,J.width,J.height,J.depth,0,ee,ye,J.data);else if(v.isFramebufferTexture){if(re)if(R)t.texStorage2D(n.TEXTURE_2D,pe,xe,J.width,J.height);else{let K=J.width,G=J.height;for(let _e=0;_e<pe;_e++)t.texImage2D(n.TEXTURE_2D,_e,xe,K,G,0,ee,ye,null),K>>=1,G>>=1}}else if(We.length>0){if(R&&re){let K=be(We[0]);t.texStorage2D(n.TEXTURE_2D,pe,xe,K.width,K.height)}for(let K=0,G=We.length;K<G;K++)de=We[K],R?te&&t.texSubImage2D(n.TEXTURE_2D,K,0,0,ee,ye,de):t.texImage2D(n.TEXTURE_2D,K,xe,ee,ye,de);v.generateMipmaps=!1}else if(R){if(re){let K=be(J);t.texStorage2D(n.TEXTURE_2D,pe,xe,K.width,K.height)}te&&t.texSubImage2D(n.TEXTURE_2D,0,0,0,ee,ye,J)}else t.texImage2D(n.TEXTURE_2D,0,xe,ee,ye,J);m(v)&&p($),ve.__version=j.version,v.onUpdate&&v.onUpdate(v)}b.__version=v.version}function ne(b,v,N){if(v.image.length!==6)return;let $=He(b,v),Y=v.source;t.bindTexture(n.TEXTURE_CUBE_MAP,b.__webglTexture,n.TEXTURE0+N);let j=i.get(Y);if(Y.version!==j.__version||$===!0){t.activeTexture(n.TEXTURE0+N);let ve=tt.getPrimaries(tt.workingColorSpace),ie=v.colorSpace===ir?null:tt.getPrimaries(v.colorSpace),we=v.colorSpace===ir||ve===ie?n.NONE:n.BROWSER_DEFAULT_WEBGL;n.pixelStorei(n.UNPACK_FLIP_Y_WEBGL,v.flipY),n.pixelStorei(n.UNPACK_PREMULTIPLY_ALPHA_WEBGL,v.premultiplyAlpha),n.pixelStorei(n.UNPACK_ALIGNMENT,v.unpackAlignment),n.pixelStorei(n.UNPACK_COLORSPACE_CONVERSION_WEBGL,we);let De=v.isCompressedTexture||v.image[0].isCompressedTexture,J=v.image[0]&&v.image[0].isDataTexture,ee=[];for(let G=0;G<6;G++)!De&&!J?ee[G]=x(v.image[G],!0,r.maxCubemapSize):ee[G]=J?v.image[G].image:v.image[G],ee[G]=xt(v,ee[G]);let ye=ee[0],xe=s.convert(v.format,v.colorSpace),de=s.convert(v.type),We=T(v.internalFormat,xe,de,v.colorSpace),R=v.isVideoTexture!==!0,re=j.__version===void 0||$===!0,te=Y.dataReady,pe=A(v,ye);fe(n.TEXTURE_CUBE_MAP,v);let K;if(De){R&&re&&t.texStorage2D(n.TEXTURE_CUBE_MAP,pe,We,ye.width,ye.height);for(let G=0;G<6;G++){K=ee[G].mipmaps;for(let _e=0;_e<K.length;_e++){let Fe=K[_e];v.format!==Yn?xe!==null?R?te&&t.compressedTexSubImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+G,_e,0,0,Fe.width,Fe.height,xe,Fe.data):t.compressedTexImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+G,_e,We,Fe.width,Fe.height,0,Fe.data):Ne("WebGLRenderer: Attempt to load unsupported compressed texture format in .setTextureCube()"):R?te&&t.texSubImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+G,_e,0,0,Fe.width,Fe.height,xe,de,Fe.data):t.texImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+G,_e,We,Fe.width,Fe.height,0,xe,de,Fe.data)}}}else{if(K=v.mipmaps,R&&re){K.length>0&&pe++;let G=be(ee[0]);t.texStorage2D(n.TEXTURE_CUBE_MAP,pe,We,G.width,G.height)}for(let G=0;G<6;G++)if(J){R?te&&t.texSubImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+G,0,0,0,ee[G].width,ee[G].height,xe,de,ee[G].data):t.texImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+G,0,We,ee[G].width,ee[G].height,0,xe,de,ee[G].data);for(let _e=0;_e<K.length;_e++){let Mt=K[_e].image[G].image;R?te&&t.texSubImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+G,_e+1,0,0,Mt.width,Mt.height,xe,de,Mt.data):t.texImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+G,_e+1,We,Mt.width,Mt.height,0,xe,de,Mt.data)}}else{R?te&&t.texSubImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+G,0,0,0,xe,de,ee[G]):t.texImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+G,0,We,xe,de,ee[G]);for(let _e=0;_e<K.length;_e++){let Fe=K[_e];R?te&&t.texSubImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+G,_e+1,0,0,xe,de,Fe.image[G]):t.texImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+G,_e+1,We,xe,de,Fe.image[G])}}}m(v)&&p(n.TEXTURE_CUBE_MAP),j.__version=Y.version,v.onUpdate&&v.onUpdate(v)}b.__version=v.version}function se(b,v,N,$,Y,j){let ve=s.convert(N.format,N.colorSpace),ie=s.convert(N.type),we=T(N.internalFormat,ve,ie,N.colorSpace),De=i.get(v),J=i.get(N);if(J.__renderTarget=v,!De.__hasExternalTextures){let ee=Math.max(1,v.width>>j),ye=Math.max(1,v.height>>j);Y===n.TEXTURE_3D||Y===n.TEXTURE_2D_ARRAY?t.texImage3D(Y,j,we,ee,ye,v.depth,0,ve,ie,null):t.texImage2D(Y,j,we,ee,ye,0,ve,ie,null)}t.bindFramebuffer(n.FRAMEBUFFER,b),Bt(v)?a.framebufferTexture2DMultisampleEXT(n.FRAMEBUFFER,$,Y,J.__webglTexture,0,C(v)):(Y===n.TEXTURE_2D||Y>=n.TEXTURE_CUBE_MAP_POSITIVE_X&&Y<=n.TEXTURE_CUBE_MAP_NEGATIVE_Z)&&n.framebufferTexture2D(n.FRAMEBUFFER,$,Y,J.__webglTexture,j),t.bindFramebuffer(n.FRAMEBUFFER,null)}function Ve(b,v,N){if(n.bindRenderbuffer(n.RENDERBUFFER,b),v.depthBuffer){let $=v.depthTexture,Y=$&&$.isDepthTexture?$.type:null,j=E(v.stencilBuffer,Y),ve=v.stencilBuffer?n.DEPTH_STENCIL_ATTACHMENT:n.DEPTH_ATTACHMENT;Bt(v)?a.renderbufferStorageMultisampleEXT(n.RENDERBUFFER,C(v),j,v.width,v.height):N?n.renderbufferStorageMultisample(n.RENDERBUFFER,C(v),j,v.width,v.height):n.renderbufferStorage(n.RENDERBUFFER,j,v.width,v.height),n.framebufferRenderbuffer(n.FRAMEBUFFER,ve,n.RENDERBUFFER,b)}else{let $=v.textures;for(let Y=0;Y<$.length;Y++){let j=$[Y],ve=s.convert(j.format,j.colorSpace),ie=s.convert(j.type),we=T(j.internalFormat,ve,ie,j.colorSpace);Bt(v)?a.renderbufferStorageMultisampleEXT(n.RENDERBUFFER,C(v),we,v.width,v.height):N?n.renderbufferStorageMultisample(n.RENDERBUFFER,C(v),we,v.width,v.height):n.renderbufferStorage(n.RENDERBUFFER,we,v.width,v.height)}}n.bindRenderbuffer(n.RENDERBUFFER,null)}function Ie(b,v,N){let $=v.isWebGLCubeRenderTarget===!0;if(t.bindFramebuffer(n.FRAMEBUFFER,b),!(v.depthTexture&&v.depthTexture.isDepthTexture))throw new Error("renderTarget.depthTexture must be an instance of THREE.DepthTexture");let Y=i.get(v.depthTexture);if(Y.__renderTarget=v,(!Y.__webglTexture||v.depthTexture.image.width!==v.width||v.depthTexture.image.height!==v.height)&&(v.depthTexture.image.width=v.width,v.depthTexture.image.height=v.height,v.depthTexture.needsUpdate=!0),$){if(Y.__webglInit===void 0&&(Y.__webglInit=!0,v.depthTexture.addEventListener("dispose",I)),Y.__webglTexture===void 0){Y.__webglTexture=n.createTexture(),t.bindTexture(n.TEXTURE_CUBE_MAP,Y.__webglTexture),fe(n.TEXTURE_CUBE_MAP,v.depthTexture);let De=s.convert(v.depthTexture.format),J=s.convert(v.depthTexture.type),ee;v.depthTexture.format===Ti?ee=n.DEPTH_COMPONENT24:v.depthTexture.format===Fr&&(ee=n.DEPTH24_STENCIL8);for(let ye=0;ye<6;ye++)n.texImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+ye,0,ee,v.width,v.height,0,De,J,null)}}else B(v.depthTexture,0);let j=Y.__webglTexture,ve=C(v),ie=$?n.TEXTURE_CUBE_MAP_POSITIVE_X+N:n.TEXTURE_2D,we=v.depthTexture.format===Fr?n.DEPTH_STENCIL_ATTACHMENT:n.DEPTH_ATTACHMENT;if(v.depthTexture.format===Ti)Bt(v)?a.framebufferTexture2DMultisampleEXT(n.FRAMEBUFFER,we,ie,j,0,ve):n.framebufferTexture2D(n.FRAMEBUFFER,we,ie,j,0);else if(v.depthTexture.format===Fr)Bt(v)?a.framebufferTexture2DMultisampleEXT(n.FRAMEBUFFER,we,ie,j,0,ve):n.framebufferTexture2D(n.FRAMEBUFFER,we,ie,j,0);else throw new Error("Unknown depthTexture format")}function Pe(b){let v=i.get(b),N=b.isWebGLCubeRenderTarget===!0;if(v.__boundDepthTexture!==b.depthTexture){let $=b.depthTexture;if(v.__depthDisposeCallback&&v.__depthDisposeCallback(),$){let Y=()=>{delete v.__boundDepthTexture,delete v.__depthDisposeCallback,$.removeEventListener("dispose",Y)};$.addEventListener("dispose",Y),v.__depthDisposeCallback=Y}v.__boundDepthTexture=$}if(b.depthTexture&&!v.__autoAllocateDepthBuffer)if(N)for(let $=0;$<6;$++)Ie(v.__webglFramebuffer[$],b,$);else{let $=b.texture.mipmaps;$&&$.length>0?Ie(v.__webglFramebuffer[0],b,0):Ie(v.__webglFramebuffer,b,0)}else if(N){v.__webglDepthbuffer=[];for(let $=0;$<6;$++)if(t.bindFramebuffer(n.FRAMEBUFFER,v.__webglFramebuffer[$]),v.__webglDepthbuffer[$]===void 0)v.__webglDepthbuffer[$]=n.createRenderbuffer(),Ve(v.__webglDepthbuffer[$],b,!1);else{let Y=b.stencilBuffer?n.DEPTH_STENCIL_ATTACHMENT:n.DEPTH_ATTACHMENT,j=v.__webglDepthbuffer[$];n.bindRenderbuffer(n.RENDERBUFFER,j),n.framebufferRenderbuffer(n.FRAMEBUFFER,Y,n.RENDERBUFFER,j)}}else{let $=b.texture.mipmaps;if($&&$.length>0?t.bindFramebuffer(n.FRAMEBUFFER,v.__webglFramebuffer[0]):t.bindFramebuffer(n.FRAMEBUFFER,v.__webglFramebuffer),v.__webglDepthbuffer===void 0)v.__webglDepthbuffer=n.createRenderbuffer(),Ve(v.__webglDepthbuffer,b,!1);else{let Y=b.stencilBuffer?n.DEPTH_STENCIL_ATTACHMENT:n.DEPTH_ATTACHMENT,j=v.__webglDepthbuffer;n.bindRenderbuffer(n.RENDERBUFFER,j),n.framebufferRenderbuffer(n.FRAMEBUFFER,Y,n.RENDERBUFFER,j)}}t.bindFramebuffer(n.FRAMEBUFFER,null)}function Yt(b,v,N){let $=i.get(b);v!==void 0&&se($.__webglFramebuffer,b,b.texture,n.COLOR_ATTACHMENT0,n.TEXTURE_2D,0),N!==void 0&&Pe(b)}function nt(b){let v=b.texture,N=i.get(b),$=i.get(v);b.addEventListener("dispose",D);let Y=b.textures,j=b.isWebGLCubeRenderTarget===!0,ve=Y.length>1;if(ve||($.__webglTexture===void 0&&($.__webglTexture=n.createTexture()),$.__version=v.version,o.memory.textures++),j){N.__webglFramebuffer=[];for(let ie=0;ie<6;ie++)if(v.mipmaps&&v.mipmaps.length>0){N.__webglFramebuffer[ie]=[];for(let we=0;we<v.mipmaps.length;we++)N.__webglFramebuffer[ie][we]=n.createFramebuffer()}else N.__webglFramebuffer[ie]=n.createFramebuffer()}else{if(v.mipmaps&&v.mipmaps.length>0){N.__webglFramebuffer=[];for(let ie=0;ie<v.mipmaps.length;ie++)N.__webglFramebuffer[ie]=n.createFramebuffer()}else N.__webglFramebuffer=n.createFramebuffer();if(ve)for(let ie=0,we=Y.length;ie<we;ie++){let De=i.get(Y[ie]);De.__webglTexture===void 0&&(De.__webglTexture=n.createTexture(),o.memory.textures++)}if(b.samples>0&&Bt(b)===!1){N.__webglMultisampledFramebuffer=n.createFramebuffer(),N.__webglColorRenderbuffer=[],t.bindFramebuffer(n.FRAMEBUFFER,N.__webglMultisampledFramebuffer);for(let ie=0;ie<Y.length;ie++){let we=Y[ie];N.__webglColorRenderbuffer[ie]=n.createRenderbuffer(),n.bindRenderbuffer(n.RENDERBUFFER,N.__webglColorRenderbuffer[ie]);let De=s.convert(we.format,we.colorSpace),J=s.convert(we.type),ee=T(we.internalFormat,De,J,we.colorSpace,b.isXRRenderTarget===!0),ye=C(b);n.renderbufferStorageMultisample(n.RENDERBUFFER,ye,ee,b.width,b.height),n.framebufferRenderbuffer(n.FRAMEBUFFER,n.COLOR_ATTACHMENT0+ie,n.RENDERBUFFER,N.__webglColorRenderbuffer[ie])}n.bindRenderbuffer(n.RENDERBUFFER,null),b.depthBuffer&&(N.__webglDepthRenderbuffer=n.createRenderbuffer(),Ve(N.__webglDepthRenderbuffer,b,!0)),t.bindFramebuffer(n.FRAMEBUFFER,null)}}if(j){t.bindTexture(n.TEXTURE_CUBE_MAP,$.__webglTexture),fe(n.TEXTURE_CUBE_MAP,v);for(let ie=0;ie<6;ie++)if(v.mipmaps&&v.mipmaps.length>0)for(let we=0;we<v.mipmaps.length;we++)se(N.__webglFramebuffer[ie][we],b,v,n.COLOR_ATTACHMENT0,n.TEXTURE_CUBE_MAP_POSITIVE_X+ie,we);else se(N.__webglFramebuffer[ie],b,v,n.COLOR_ATTACHMENT0,n.TEXTURE_CUBE_MAP_POSITIVE_X+ie,0);m(v)&&p(n.TEXTURE_CUBE_MAP),t.unbindTexture()}else if(ve){for(let ie=0,we=Y.length;ie<we;ie++){let De=Y[ie],J=i.get(De),ee=n.TEXTURE_2D;(b.isWebGL3DRenderTarget||b.isWebGLArrayRenderTarget)&&(ee=b.isWebGL3DRenderTarget?n.TEXTURE_3D:n.TEXTURE_2D_ARRAY),t.bindTexture(ee,J.__webglTexture),fe(ee,De),se(N.__webglFramebuffer,b,De,n.COLOR_ATTACHMENT0+ie,ee,0),m(De)&&p(ee)}t.unbindTexture()}else{let ie=n.TEXTURE_2D;if((b.isWebGL3DRenderTarget||b.isWebGLArrayRenderTarget)&&(ie=b.isWebGL3DRenderTarget?n.TEXTURE_3D:n.TEXTURE_2D_ARRAY),t.bindTexture(ie,$.__webglTexture),fe(ie,v),v.mipmaps&&v.mipmaps.length>0)for(let we=0;we<v.mipmaps.length;we++)se(N.__webglFramebuffer[we],b,v,n.COLOR_ATTACHMENT0,ie,we);else se(N.__webglFramebuffer,b,v,n.COLOR_ATTACHMENT0,ie,0);m(v)&&p(ie),t.unbindTexture()}b.depthBuffer&&Pe(b)}function dt(b){let v=b.textures;for(let N=0,$=v.length;N<$;N++){let Y=v[N];if(m(Y)){let j=M(b),ve=i.get(Y).__webglTexture;t.bindTexture(j,ve),p(j),t.unbindTexture()}}}let yt=[],Ge=[];function Lt(b){if(b.samples>0){if(Bt(b)===!1){let v=b.textures,N=b.width,$=b.height,Y=n.COLOR_BUFFER_BIT,j=b.stencilBuffer?n.DEPTH_STENCIL_ATTACHMENT:n.DEPTH_ATTACHMENT,ve=i.get(b),ie=v.length>1;if(ie)for(let De=0;De<v.length;De++)t.bindFramebuffer(n.FRAMEBUFFER,ve.__webglMultisampledFramebuffer),n.framebufferRenderbuffer(n.FRAMEBUFFER,n.COLOR_ATTACHMENT0+De,n.RENDERBUFFER,null),t.bindFramebuffer(n.FRAMEBUFFER,ve.__webglFramebuffer),n.framebufferTexture2D(n.DRAW_FRAMEBUFFER,n.COLOR_ATTACHMENT0+De,n.TEXTURE_2D,null,0);t.bindFramebuffer(n.READ_FRAMEBUFFER,ve.__webglMultisampledFramebuffer);let we=b.texture.mipmaps;we&&we.length>0?t.bindFramebuffer(n.DRAW_FRAMEBUFFER,ve.__webglFramebuffer[0]):t.bindFramebuffer(n.DRAW_FRAMEBUFFER,ve.__webglFramebuffer);for(let De=0;De<v.length;De++){if(b.resolveDepthBuffer&&(b.depthBuffer&&(Y|=n.DEPTH_BUFFER_BIT),b.stencilBuffer&&b.resolveStencilBuffer&&(Y|=n.STENCIL_BUFFER_BIT)),ie){n.framebufferRenderbuffer(n.READ_FRAMEBUFFER,n.COLOR_ATTACHMENT0,n.RENDERBUFFER,ve.__webglColorRenderbuffer[De]);let J=i.get(v[De]).__webglTexture;n.framebufferTexture2D(n.DRAW_FRAMEBUFFER,n.COLOR_ATTACHMENT0,n.TEXTURE_2D,J,0)}n.blitFramebuffer(0,0,N,$,0,0,N,$,Y,n.NEAREST),c===!0&&(yt.length=0,Ge.length=0,yt.push(n.COLOR_ATTACHMENT0+De),b.depthBuffer&&b.resolveDepthBuffer===!1&&(yt.push(j),Ge.push(j),n.invalidateFramebuffer(n.DRAW_FRAMEBUFFER,Ge)),n.invalidateFramebuffer(n.READ_FRAMEBUFFER,yt))}if(t.bindFramebuffer(n.READ_FRAMEBUFFER,null),t.bindFramebuffer(n.DRAW_FRAMEBUFFER,null),ie)for(let De=0;De<v.length;De++){t.bindFramebuffer(n.FRAMEBUFFER,ve.__webglMultisampledFramebuffer),n.framebufferRenderbuffer(n.FRAMEBUFFER,n.COLOR_ATTACHMENT0+De,n.RENDERBUFFER,ve.__webglColorRenderbuffer[De]);let J=i.get(v[De]).__webglTexture;t.bindFramebuffer(n.FRAMEBUFFER,ve.__webglFramebuffer),n.framebufferTexture2D(n.DRAW_FRAMEBUFFER,n.COLOR_ATTACHMENT0+De,n.TEXTURE_2D,J,0)}t.bindFramebuffer(n.DRAW_FRAMEBUFFER,ve.__webglMultisampledFramebuffer)}else if(b.depthBuffer&&b.resolveDepthBuffer===!1&&c){let v=b.stencilBuffer?n.DEPTH_STENCIL_ATTACHMENT:n.DEPTH_ATTACHMENT;n.invalidateFramebuffer(n.DRAW_FRAMEBUFFER,[v])}}}function C(b){return Math.min(r.maxSamples,b.samples)}function Bt(b){let v=i.get(b);return b.samples>0&&e.has("WEBGL_multisampled_render_to_texture")===!0&&v.__useRenderToTexture!==!1}function lt(b){let v=o.render.frame;u.get(b)!==v&&(u.set(b,v),b.update())}function xt(b,v){let N=b.colorSpace,$=b.format,Y=b.type;return b.isCompressedTexture===!0||b.isVideoTexture===!0||N!==Ms&&N!==ir&&(tt.getTransfer(N)===ut?($!==Yn||Y!==On)&&Ne("WebGLTextures: sRGB encoded textures have to use RGBAFormat and UnsignedByteType."):Re("WebGLTextures: Unsupported texture color space:",N)),v}function be(b){return typeof HTMLImageElement<"u"&&b instanceof HTMLImageElement?(l.width=b.naturalWidth||b.width,l.height=b.naturalHeight||b.height):typeof VideoFrame<"u"&&b instanceof VideoFrame?(l.width=b.displayWidth,l.height=b.displayHeight):(l.width=b.width,l.height=b.height),l}this.allocateTextureUnit=V,this.resetTextureUnits=U,this.setTexture2D=B,this.setTexture2DArray=H,this.setTexture3D=L,this.setTextureCube=Q,this.rebindTextures=Yt,this.setupRenderTarget=nt,this.updateRenderTargetMipmap=dt,this.updateMultisampleRenderTarget=Lt,this.setupDepthRenderbuffer=Pe,this.setupFrameBufferTexture=se,this.useMultisampledRTT=Bt,this.isReversedDepthBuffer=function(){return t.buffers.depth.getReversed()}}function xR(n,e){function t(i,r=ir){let s,o=tt.getTransfer(r);if(i===On)return n.UNSIGNED_BYTE;if(i===Zu)return n.UNSIGNED_SHORT_4_4_4_4;if(i===Ju)return n.UNSIGNED_SHORT_5_5_5_1;if(i===eg)return n.UNSIGNED_INT_5_9_9_9_REV;if(i===tg)return n.UNSIGNED_INT_10F_11F_11F_REV;if(i===Km)return n.BYTE;if(i===Qm)return n.SHORT;if(i===Io)return n.UNSIGNED_SHORT;if(i===Yu)return n.INT;if(i===fi)return n.UNSIGNED_INT;if(i===hi)return n.FLOAT;if(i===Di)return n.HALF_FLOAT;if(i===ng)return n.ALPHA;if(i===ig)return n.RGB;if(i===Yn)return n.RGBA;if(i===Ti)return n.DEPTH_COMPONENT;if(i===Fr)return n.DEPTH_STENCIL;if(i===rg)return n.RED;if(i===Ku)return n.RED_INTEGER;if(i===Ts)return n.RG;if(i===Qu)return n.RG_INTEGER;if(i===ed)return n.RGBA_INTEGER;if(i===Ka||i===Qa||i===ec||i===tc)if(o===ut)if(s=e.get("WEBGL_compressed_texture_s3tc_srgb"),s!==null){if(i===Ka)return s.COMPRESSED_SRGB_S3TC_DXT1_EXT;if(i===Qa)return s.COMPRESSED_SRGB_ALPHA_S3TC_DXT1_EXT;if(i===ec)return s.COMPRESSED_SRGB_ALPHA_S3TC_DXT3_EXT;if(i===tc)return s.COMPRESSED_SRGB_ALPHA_S3TC_DXT5_EXT}else return null;else if(s=e.get("WEBGL_compressed_texture_s3tc"),s!==null){if(i===Ka)return s.COMPRESSED_RGB_S3TC_DXT1_EXT;if(i===Qa)return s.COMPRESSED_RGBA_S3TC_DXT1_EXT;if(i===ec)return s.COMPRESSED_RGBA_S3TC_DXT3_EXT;if(i===tc)return s.COMPRESSED_RGBA_S3TC_DXT5_EXT}else return null;if(i===td||i===nd||i===id||i===rd)if(s=e.get("WEBGL_compressed_texture_pvrtc"),s!==null){if(i===td)return s.COMPRESSED_RGB_PVRTC_4BPPV1_IMG;if(i===nd)return s.COMPRESSED_RGB_PVRTC_2BPPV1_IMG;if(i===id)return s.COMPRESSED_RGBA_PVRTC_4BPPV1_IMG;if(i===rd)return s.COMPRESSED_RGBA_PVRTC_2BPPV1_IMG}else return null;if(i===sd||i===od||i===ad||i===cd||i===ld||i===ud||i===dd)if(s=e.get("WEBGL_compressed_texture_etc"),s!==null){if(i===sd||i===od)return o===ut?s.COMPRESSED_SRGB8_ETC2:s.COMPRESSED_RGB8_ETC2;if(i===ad)return o===ut?s.COMPRESSED_SRGB8_ALPHA8_ETC2_EAC:s.COMPRESSED_RGBA8_ETC2_EAC;if(i===cd)return s.COMPRESSED_R11_EAC;if(i===ld)return s.COMPRESSED_SIGNED_R11_EAC;if(i===ud)return s.COMPRESSED_RG11_EAC;if(i===dd)return s.COMPRESSED_SIGNED_RG11_EAC}else return null;if(i===fd||i===hd||i===pd||i===md||i===gd||i===vd||i===yd||i===_d||i===xd||i===Md||i===bd||i===Sd||i===Ed||i===Td)if(s=e.get("WEBGL_compressed_texture_astc"),s!==null){if(i===fd)return o===ut?s.COMPRESSED_SRGB8_ALPHA8_ASTC_4x4_KHR:s.COMPRESSED_RGBA_ASTC_4x4_KHR;if(i===hd)return o===ut?s.COMPRESSED_SRGB8_ALPHA8_ASTC_5x4_KHR:s.COMPRESSED_RGBA_ASTC_5x4_KHR;if(i===pd)return o===ut?s.COMPRESSED_SRGB8_ALPHA8_ASTC_5x5_KHR:s.COMPRESSED_RGBA_ASTC_5x5_KHR;if(i===md)return o===ut?s.COMPRESSED_SRGB8_ALPHA8_ASTC_6x5_KHR:s.COMPRESSED_RGBA_ASTC_6x5_KHR;if(i===gd)return o===ut?s.COMPRESSED_SRGB8_ALPHA8_ASTC_6x6_KHR:s.COMPRESSED_RGBA_ASTC_6x6_KHR;if(i===vd)return o===ut?s.COMPRESSED_SRGB8_ALPHA8_ASTC_8x5_KHR:s.COMPRESSED_RGBA_ASTC_8x5_KHR;if(i===yd)return o===ut?s.COMPRESSED_SRGB8_ALPHA8_ASTC_8x6_KHR:s.COMPRESSED_RGBA_ASTC_8x6_KHR;if(i===_d)return o===ut?s.COMPRESSED_SRGB8_ALPHA8_ASTC_8x8_KHR:s.COMPRESSED_RGBA_ASTC_8x8_KHR;if(i===xd)return o===ut?s.COMPRESSED_SRGB8_ALPHA8_ASTC_10x5_KHR:s.COMPRESSED_RGBA_ASTC_10x5_KHR;if(i===Md)return o===ut?s.COMPRESSED_SRGB8_ALPHA8_ASTC_10x6_KHR:s.COMPRESSED_RGBA_ASTC_10x6_KHR;if(i===bd)return o===ut?s.COMPRESSED_SRGB8_ALPHA8_ASTC_10x8_KHR:s.COMPRESSED_RGBA_ASTC_10x8_KHR;if(i===Sd)return o===ut?s.COMPRESSED_SRGB8_ALPHA8_ASTC_10x10_KHR:s.COMPRESSED_RGBA_ASTC_10x10_KHR;if(i===Ed)return o===ut?s.COMPRESSED_SRGB8_ALPHA8_ASTC_12x10_KHR:s.COMPRESSED_RGBA_ASTC_12x10_KHR;if(i===Td)return o===ut?s.COMPRESSED_SRGB8_ALPHA8_ASTC_12x12_KHR:s.COMPRESSED_RGBA_ASTC_12x12_KHR}else return null;if(i===wd||i===Cd||i===Id)if(s=e.get("EXT_texture_compression_bptc"),s!==null){if(i===wd)return o===ut?s.COMPRESSED_SRGB_ALPHA_BPTC_UNORM_EXT:s.COMPRESSED_RGBA_BPTC_UNORM_EXT;if(i===Cd)return s.COMPRESSED_RGB_BPTC_SIGNED_FLOAT_EXT;if(i===Id)return s.COMPRESSED_RGB_BPTC_UNSIGNED_FLOAT_EXT}else return null;if(i===Ad||i===Dd||i===Rd||i===Nd)if(s=e.get("EXT_texture_compression_rgtc"),s!==null){if(i===Ad)return s.COMPRESSED_RED_RGTC1_EXT;if(i===Dd)return s.COMPRESSED_SIGNED_RED_RGTC1_EXT;if(i===Rd)return s.COMPRESSED_RED_GREEN_RGTC2_EXT;if(i===Nd)return s.COMPRESSED_SIGNED_RED_GREEN_RGTC2_EXT}else return null;return i===Ao?n.UNSIGNED_INT_24_8:n[i]!==void 0?n[i]:null}return{convert:t}}var MR=`
void main() {

	gl_Position = vec4( position, 1.0 );

}`,bR=`
uniform sampler2DArray depthColor;
uniform float depthWidth;
uniform float depthHeight;

void main() {

	vec2 coord = vec2( gl_FragCoord.x / depthWidth, gl_FragCoord.y / depthHeight );

	if ( coord.x >= 1.0 ) {

		gl_FragDepth = texture( depthColor, vec3( coord.x - 1.0, coord.y, 1 ) ).r;

	} else {

		gl_FragDepth = texture( depthColor, vec3( coord.x, coord.y, 0 ) ).r;

	}

}`,Eg=class{constructor(){this.texture=null,this.mesh=null,this.depthNear=0,this.depthFar=0}init(e,t){if(this.texture===null){let i=new Ga(e.texture);(e.depthNear!==t.depthNear||e.depthFar!==t.depthFar)&&(this.depthNear=e.depthNear,this.depthFar=e.depthFar),this.texture=i}}getMesh(e){if(this.texture!==null&&this.mesh===null){let t=e.cameras[0].viewport,i=new Pn({vertexShader:MR,fragmentShader:bR,uniforms:{depthColor:{value:this.texture},depthWidth:{value:t.z},depthHeight:{value:t.w}}});this.mesh=new Xn(new Wa(20,20),i)}return this.mesh}reset(){this.texture=null,this.mesh=null}getDepthTexture(){return this.texture}},Tg=class extends nr{constructor(e,t){super();let i=this,r=null,s=1,o=null,a="local-floor",c=1,l=null,u=null,f=null,d=null,h=null,g=null,x=typeof XRWebGLBinding<"u",m=new Eg,p={},M=t.getContextAttributes(),T=null,E=null,A=[],I=[],D=new ht,y=null,S=new fn;S.viewport=new Pt;let q=new fn;q.viewport=new Pt;let w=[S,q],U=new ju,V=null,W=null;this.cameraAutoUpdate=!0,this.enabled=!1,this.isPresenting=!1,this.getController=function(X){let ne=A[X];return ne===void 0&&(ne=new Eo,A[X]=ne),ne.getTargetRaySpace()},this.getControllerGrip=function(X){let ne=A[X];return ne===void 0&&(ne=new Eo,A[X]=ne),ne.getGripSpace()},this.getHand=function(X){let ne=A[X];return ne===void 0&&(ne=new Eo,A[X]=ne),ne.getHandSpace()};function B(X){let ne=I.indexOf(X.inputSource);if(ne===-1)return;let se=A[ne];se!==void 0&&(se.update(X.inputSource,X.frame,l||o),se.dispatchEvent({type:X.type,data:X.inputSource}))}function H(){r.removeEventListener("select",B),r.removeEventListener("selectstart",B),r.removeEventListener("selectend",B),r.removeEventListener("squeeze",B),r.removeEventListener("squeezestart",B),r.removeEventListener("squeezeend",B),r.removeEventListener("end",H),r.removeEventListener("inputsourceschange",L);for(let X=0;X<A.length;X++){let ne=I[X];ne!==null&&(I[X]=null,A[X].disconnect(ne))}V=null,W=null,m.reset();for(let X in p)delete p[X];e.setRenderTarget(T),h=null,d=null,f=null,r=null,E=null,St.stop(),i.isPresenting=!1,e.setPixelRatio(y),e.setSize(D.width,D.height,!1),i.dispatchEvent({type:"sessionend"})}this.setFramebufferScaleFactor=function(X){s=X,i.isPresenting===!0&&Ne("WebXRManager: Cannot change framebuffer scale while presenting.")},this.setReferenceSpaceType=function(X){a=X,i.isPresenting===!0&&Ne("WebXRManager: Cannot change reference space type while presenting.")},this.getReferenceSpace=function(){return l||o},this.setReferenceSpace=function(X){l=X},this.getBaseLayer=function(){return d!==null?d:h},this.getBinding=function(){return f===null&&x&&(f=new XRWebGLBinding(r,t)),f},this.getFrame=function(){return g},this.getSession=function(){return r},this.setSession=async function(X){if(r=X,r!==null){if(T=e.getRenderTarget(),r.addEventListener("select",B),r.addEventListener("selectstart",B),r.addEventListener("selectend",B),r.addEventListener("squeeze",B),r.addEventListener("squeezestart",B),r.addEventListener("squeezeend",B),r.addEventListener("end",H),r.addEventListener("inputsourceschange",L),M.xrCompatible!==!0&&await t.makeXRCompatible(),y=e.getPixelRatio(),e.getSize(D),x&&"createProjectionLayer"in XRWebGLBinding.prototype){let se=null,Ve=null,Ie=null;M.depth&&(Ie=M.stencil?t.DEPTH24_STENCIL8:t.DEPTH_COMPONENT24,se=M.stencil?Fr:Ti,Ve=M.stencil?Ao:fi);let Pe={colorFormat:t.RGBA8,depthFormat:Ie,scaleFactor:s};f=this.getBinding(),d=f.createProjectionLayer(Pe),r.updateRenderState({layers:[d]}),e.setPixelRatio(1),e.setSize(d.textureWidth,d.textureHeight,!1),E=new Rn(d.textureWidth,d.textureHeight,{format:Yn,type:On,depthTexture:new Dr(d.textureWidth,d.textureHeight,Ve,void 0,void 0,void 0,void 0,void 0,void 0,se),stencilBuffer:M.stencil,colorSpace:e.outputColorSpace,samples:M.antialias?4:0,resolveDepthBuffer:d.ignoreDepthValues===!1,resolveStencilBuffer:d.ignoreDepthValues===!1})}else{let se={antialias:M.antialias,alpha:!0,depth:M.depth,stencil:M.stencil,framebufferScaleFactor:s};h=new XRWebGLLayer(r,t,se),r.updateRenderState({baseLayer:h}),e.setPixelRatio(1),e.setSize(h.framebufferWidth,h.framebufferHeight,!1),E=new Rn(h.framebufferWidth,h.framebufferHeight,{format:Yn,type:On,colorSpace:e.outputColorSpace,stencilBuffer:M.stencil,resolveDepthBuffer:h.ignoreDepthValues===!1,resolveStencilBuffer:h.ignoreDepthValues===!1})}E.isXRRenderTarget=!0,this.setFoveation(c),l=null,o=await r.requestReferenceSpace(a),St.setContext(r),St.start(),i.isPresenting=!0,i.dispatchEvent({type:"sessionstart"})}},this.getEnvironmentBlendMode=function(){if(r!==null)return r.environmentBlendMode},this.getDepthTexture=function(){return m.getDepthTexture()};function L(X){for(let ne=0;ne<X.removed.length;ne++){let se=X.removed[ne],Ve=I.indexOf(se);Ve>=0&&(I[Ve]=null,A[Ve].disconnect(se))}for(let ne=0;ne<X.added.length;ne++){let se=X.added[ne],Ve=I.indexOf(se);if(Ve===-1){for(let Pe=0;Pe<A.length;Pe++)if(Pe>=I.length){I.push(se),Ve=Pe;break}else if(I[Pe]===null){I[Pe]=se,Ve=Pe;break}if(Ve===-1)break}let Ie=A[Ve];Ie&&Ie.connect(se)}}let Q=new k,Z=new k;function ue(X,ne,se){Q.setFromMatrixPosition(ne.matrixWorld),Z.setFromMatrixPosition(se.matrixWorld);let Ve=Q.distanceTo(Z),Ie=ne.projectionMatrix.elements,Pe=se.projectionMatrix.elements,Yt=Ie[14]/(Ie[10]-1),nt=Ie[14]/(Ie[10]+1),dt=(Ie[9]+1)/Ie[5],yt=(Ie[9]-1)/Ie[5],Ge=(Ie[8]-1)/Ie[0],Lt=(Pe[8]+1)/Pe[0],C=Yt*Ge,Bt=Yt*Lt,lt=Ve/(-Ge+Lt),xt=lt*-Ge;if(ne.matrixWorld.decompose(X.position,X.quaternion,X.scale),X.translateX(xt),X.translateZ(lt),X.matrixWorld.compose(X.position,X.quaternion,X.scale),X.matrixWorldInverse.copy(X.matrixWorld).invert(),Ie[10]===-1)X.projectionMatrix.copy(ne.projectionMatrix),X.projectionMatrixInverse.copy(ne.projectionMatrixInverse);else{let be=Yt+lt,b=nt+lt,v=C-xt,N=Bt+(Ve-xt),$=dt*nt/b*be,Y=yt*nt/b*be;X.projectionMatrix.makePerspective(v,N,$,Y,be,b),X.projectionMatrixInverse.copy(X.projectionMatrix).invert()}}function ge(X,ne){ne===null?X.matrixWorld.copy(X.matrix):X.matrixWorld.multiplyMatrices(ne.matrixWorld,X.matrix),X.matrixWorldInverse.copy(X.matrixWorld).invert()}this.updateCamera=function(X){if(r===null)return;let ne=X.near,se=X.far;m.texture!==null&&(m.depthNear>0&&(ne=m.depthNear),m.depthFar>0&&(se=m.depthFar)),U.near=q.near=S.near=ne,U.far=q.far=S.far=se,(V!==U.near||W!==U.far)&&(r.updateRenderState({depthNear:U.near,depthFar:U.far}),V=U.near,W=U.far),U.layers.mask=X.layers.mask|6,S.layers.mask=U.layers.mask&-5,q.layers.mask=U.layers.mask&-3;let Ve=X.parent,Ie=U.cameras;ge(U,Ve);for(let Pe=0;Pe<Ie.length;Pe++)ge(Ie[Pe],Ve);Ie.length===2?ue(U,S,q):U.projectionMatrix.copy(S.projectionMatrix),fe(X,U,Ve)};function fe(X,ne,se){se===null?X.matrix.copy(ne.matrixWorld):(X.matrix.copy(se.matrixWorld),X.matrix.invert(),X.matrix.multiply(ne.matrixWorld)),X.matrix.decompose(X.position,X.quaternion,X.scale),X.updateMatrixWorld(!0),X.projectionMatrix.copy(ne.projectionMatrix),X.projectionMatrixInverse.copy(ne.projectionMatrixInverse),X.isPerspectiveCamera&&(X.fov=wu*2*Math.atan(1/X.projectionMatrix.elements[5]),X.zoom=1)}this.getCamera=function(){return U},this.getFoveation=function(){if(!(d===null&&h===null))return c},this.setFoveation=function(X){c=X,d!==null&&(d.fixedFoveation=X),h!==null&&h.fixedFoveation!==void 0&&(h.fixedFoveation=X)},this.hasDepthSensing=function(){return m.texture!==null},this.getDepthSensingMesh=function(){return m.getMesh(U)},this.getCameraTexture=function(X){return p[X]};let He=null;function Tt(X,ne){if(u=ne.getViewerPose(l||o),g=ne,u!==null){let se=u.views;h!==null&&(e.setRenderTargetFramebuffer(E,h.framebuffer),e.setRenderTarget(E));let Ve=!1;se.length!==U.cameras.length&&(U.cameras.length=0,Ve=!0);for(let nt=0;nt<se.length;nt++){let dt=se[nt],yt=null;if(h!==null)yt=h.getViewport(dt);else{let Lt=f.getViewSubImage(d,dt);yt=Lt.viewport,nt===0&&(e.setRenderTargetTextures(E,Lt.colorTexture,Lt.depthStencilTexture),e.setRenderTarget(E))}let Ge=w[nt];Ge===void 0&&(Ge=new fn,Ge.layers.enable(nt),Ge.viewport=new Pt,w[nt]=Ge),Ge.matrix.fromArray(dt.transform.matrix),Ge.matrix.decompose(Ge.position,Ge.quaternion,Ge.scale),Ge.projectionMatrix.fromArray(dt.projectionMatrix),Ge.projectionMatrixInverse.copy(Ge.projectionMatrix).invert(),Ge.viewport.set(yt.x,yt.y,yt.width,yt.height),nt===0&&(U.matrix.copy(Ge.matrix),U.matrix.decompose(U.position,U.quaternion,U.scale)),Ve===!0&&U.cameras.push(Ge)}let Ie=r.enabledFeatures;if(Ie&&Ie.includes("depth-sensing")&&r.depthUsage=="gpu-optimized"&&x){f=i.getBinding();let nt=f.getDepthInformation(se[0]);nt&&nt.isValid&&nt.texture&&m.init(nt,r.renderState)}if(Ie&&Ie.includes("camera-access")&&x){e.state.unbindTexture(),f=i.getBinding();for(let nt=0;nt<se.length;nt++){let dt=se[nt].camera;if(dt){let yt=p[dt];yt||(yt=new Ga,p[dt]=yt);let Ge=f.getCameraImage(dt);yt.sourceTexture=Ge}}}}for(let se=0;se<A.length;se++){let Ve=I[se],Ie=A[se];Ve!==null&&Ie!==void 0&&Ie.update(Ve,ne,l||o)}He&&He(X,ne),ne.detectedPlanes&&i.dispatchEvent({type:"planesdetected",data:ne}),g=null}let St=new hM;St.setAnimationLoop(Tt),this.setAnimationLoop=function(X){He=X},this.dispose=function(){}}},Is=new bs,SR=new Ut;function ER(n,e){function t(m,p){m.matrixAutoUpdate===!0&&m.updateMatrix(),p.value.copy(m.matrix)}function i(m,p){p.color.getRGB(m.fogColor.value,ag(n)),p.isFog?(m.fogNear.value=p.near,m.fogFar.value=p.far):p.isFogExp2&&(m.fogDensity.value=p.density)}function r(m,p,M,T,E){p.isMeshBasicMaterial?s(m,p):p.isMeshLambertMaterial?(s(m,p),p.envMap&&(m.envMapIntensity.value=p.envMapIntensity)):p.isMeshToonMaterial?(s(m,p),f(m,p)):p.isMeshPhongMaterial?(s(m,p),u(m,p),p.envMap&&(m.envMapIntensity.value=p.envMapIntensity)):p.isMeshStandardMaterial?(s(m,p),d(m,p),p.isMeshPhysicalMaterial&&h(m,p,E)):p.isMeshMatcapMaterial?(s(m,p),g(m,p)):p.isMeshDepthMaterial?s(m,p):p.isMeshDistanceMaterial?(s(m,p),x(m,p)):p.isMeshNormalMaterial?s(m,p):p.isLineBasicMaterial?(o(m,p),p.isLineDashedMaterial&&a(m,p)):p.isPointsMaterial?c(m,p,M,T):p.isSpriteMaterial?l(m,p):p.isShadowMaterial?(m.color.value.copy(p.color),m.opacity.value=p.opacity):p.isShaderMaterial&&(p.uniformsNeedUpdate=!1)}function s(m,p){m.opacity.value=p.opacity,p.color&&m.diffuse.value.copy(p.color),p.emissive&&m.emissive.value.copy(p.emissive).multiplyScalar(p.emissiveIntensity),p.map&&(m.map.value=p.map,t(p.map,m.mapTransform)),p.alphaMap&&(m.alphaMap.value=p.alphaMap,t(p.alphaMap,m.alphaMapTransform)),p.bumpMap&&(m.bumpMap.value=p.bumpMap,t(p.bumpMap,m.bumpMapTransform),m.bumpScale.value=p.bumpScale,p.side===vn&&(m.bumpScale.value*=-1)),p.normalMap&&(m.normalMap.value=p.normalMap,t(p.normalMap,m.normalMapTransform),m.normalScale.value.copy(p.normalScale),p.side===vn&&m.normalScale.value.negate()),p.displacementMap&&(m.displacementMap.value=p.displacementMap,t(p.displacementMap,m.displacementMapTransform),m.displacementScale.value=p.displacementScale,m.displacementBias.value=p.displacementBias),p.emissiveMap&&(m.emissiveMap.value=p.emissiveMap,t(p.emissiveMap,m.emissiveMapTransform)),p.specularMap&&(m.specularMap.value=p.specularMap,t(p.specularMap,m.specularMapTransform)),p.alphaTest>0&&(m.alphaTest.value=p.alphaTest);let M=e.get(p),T=M.envMap,E=M.envMapRotation;T&&(m.envMap.value=T,Is.copy(E),Is.x*=-1,Is.y*=-1,Is.z*=-1,T.isCubeTexture&&T.isRenderTargetTexture===!1&&(Is.y*=-1,Is.z*=-1),m.envMapRotation.value.setFromMatrix4(SR.makeRotationFromEuler(Is)),m.flipEnvMap.value=T.isCubeTexture&&T.isRenderTargetTexture===!1?-1:1,m.reflectivity.value=p.reflectivity,m.ior.value=p.ior,m.refractionRatio.value=p.refractionRatio),p.lightMap&&(m.lightMap.value=p.lightMap,m.lightMapIntensity.value=p.lightMapIntensity,t(p.lightMap,m.lightMapTransform)),p.aoMap&&(m.aoMap.value=p.aoMap,m.aoMapIntensity.value=p.aoMapIntensity,t(p.aoMap,m.aoMapTransform))}function o(m,p){m.diffuse.value.copy(p.color),m.opacity.value=p.opacity,p.map&&(m.map.value=p.map,t(p.map,m.mapTransform))}function a(m,p){m.dashSize.value=p.dashSize,m.totalSize.value=p.dashSize+p.gapSize,m.scale.value=p.scale}function c(m,p,M,T){m.diffuse.value.copy(p.color),m.opacity.value=p.opacity,m.size.value=p.size*M,m.scale.value=T*.5,p.map&&(m.map.value=p.map,t(p.map,m.uvTransform)),p.alphaMap&&(m.alphaMap.value=p.alphaMap,t(p.alphaMap,m.alphaMapTransform)),p.alphaTest>0&&(m.alphaTest.value=p.alphaTest)}function l(m,p){m.diffuse.value.copy(p.color),m.opacity.value=p.opacity,m.rotation.value=p.rotation,p.map&&(m.map.value=p.map,t(p.map,m.mapTransform)),p.alphaMap&&(m.alphaMap.value=p.alphaMap,t(p.alphaMap,m.alphaMapTransform)),p.alphaTest>0&&(m.alphaTest.value=p.alphaTest)}function u(m,p){m.specular.value.copy(p.specular),m.shininess.value=Math.max(p.shininess,1e-4)}function f(m,p){p.gradientMap&&(m.gradientMap.value=p.gradientMap)}function d(m,p){m.metalness.value=p.metalness,p.metalnessMap&&(m.metalnessMap.value=p.metalnessMap,t(p.metalnessMap,m.metalnessMapTransform)),m.roughness.value=p.roughness,p.roughnessMap&&(m.roughnessMap.value=p.roughnessMap,t(p.roughnessMap,m.roughnessMapTransform)),p.envMap&&(m.envMapIntensity.value=p.envMapIntensity)}function h(m,p,M){m.ior.value=p.ior,p.sheen>0&&(m.sheenColor.value.copy(p.sheenColor).multiplyScalar(p.sheen),m.sheenRoughness.value=p.sheenRoughness,p.sheenColorMap&&(m.sheenColorMap.value=p.sheenColorMap,t(p.sheenColorMap,m.sheenColorMapTransform)),p.sheenRoughnessMap&&(m.sheenRoughnessMap.value=p.sheenRoughnessMap,t(p.sheenRoughnessMap,m.sheenRoughnessMapTransform))),p.clearcoat>0&&(m.clearcoat.value=p.clearcoat,m.clearcoatRoughness.value=p.clearcoatRoughness,p.clearcoatMap&&(m.clearcoatMap.value=p.clearcoatMap,t(p.clearcoatMap,m.clearcoatMapTransform)),p.clearcoatRoughnessMap&&(m.clearcoatRoughnessMap.value=p.clearcoatRoughnessMap,t(p.clearcoatRoughnessMap,m.clearcoatRoughnessMapTransform)),p.clearcoatNormalMap&&(m.clearcoatNormalMap.value=p.clearcoatNormalMap,t(p.clearcoatNormalMap,m.clearcoatNormalMapTransform),m.clearcoatNormalScale.value.copy(p.clearcoatNormalScale),p.side===vn&&m.clearcoatNormalScale.value.negate())),p.dispersion>0&&(m.dispersion.value=p.dispersion),p.iridescence>0&&(m.iridescence.value=p.iridescence,m.iridescenceIOR.value=p.iridescenceIOR,m.iridescenceThicknessMinimum.value=p.iridescenceThicknessRange[0],m.iridescenceThicknessMaximum.value=p.iridescenceThicknessRange[1],p.iridescenceMap&&(m.iridescenceMap.value=p.iridescenceMap,t(p.iridescenceMap,m.iridescenceMapTransform)),p.iridescenceThicknessMap&&(m.iridescenceThicknessMap.value=p.iridescenceThicknessMap,t(p.iridescenceThicknessMap,m.iridescenceThicknessMapTransform))),p.transmission>0&&(m.transmission.value=p.transmission,m.transmissionSamplerMap.value=M.texture,m.transmissionSamplerSize.value.set(M.width,M.height),p.transmissionMap&&(m.transmissionMap.value=p.transmissionMap,t(p.transmissionMap,m.transmissionMapTransform)),m.thickness.value=p.thickness,p.thicknessMap&&(m.thicknessMap.value=p.thicknessMap,t(p.thicknessMap,m.thicknessMapTransform)),m.attenuationDistance.value=p.attenuationDistance,m.attenuationColor.value.copy(p.attenuationColor)),p.anisotropy>0&&(m.anisotropyVector.value.set(p.anisotropy*Math.cos(p.anisotropyRotation),p.anisotropy*Math.sin(p.anisotropyRotation)),p.anisotropyMap&&(m.anisotropyMap.value=p.anisotropyMap,t(p.anisotropyMap,m.anisotropyMapTransform))),m.specularIntensity.value=p.specularIntensity,m.specularColor.value.copy(p.specularColor),p.specularColorMap&&(m.specularColorMap.value=p.specularColorMap,t(p.specularColorMap,m.specularColorMapTransform)),p.specularIntensityMap&&(m.specularIntensityMap.value=p.specularIntensityMap,t(p.specularIntensityMap,m.specularIntensityMapTransform))}function g(m,p){p.matcap&&(m.matcap.value=p.matcap)}function x(m,p){let M=e.get(p).light;m.referencePosition.value.setFromMatrixPosition(M.matrixWorld),m.nearDistance.value=M.shadow.camera.near,m.farDistance.value=M.shadow.camera.far}return{refreshFogUniforms:i,refreshMaterialUniforms:r}}function TR(n,e,t,i){let r={},s={},o=[],a=n.getParameter(n.MAX_UNIFORM_BUFFER_BINDINGS);function c(M,T){let E=T.program;i.uniformBlockBinding(M,E)}function l(M,T){let E=r[M.id];E===void 0&&(g(M),E=u(M),r[M.id]=E,M.addEventListener("dispose",m));let A=T.program;i.updateUBOMapping(M,A);let I=e.render.frame;s[M.id]!==I&&(d(M),s[M.id]=I)}function u(M){let T=f();M.__bindingPointIndex=T;let E=n.createBuffer(),A=M.__size,I=M.usage;return n.bindBuffer(n.UNIFORM_BUFFER,E),n.bufferData(n.UNIFORM_BUFFER,A,I),n.bindBuffer(n.UNIFORM_BUFFER,null),n.bindBufferBase(n.UNIFORM_BUFFER,T,E),E}function f(){for(let M=0;M<a;M++)if(o.indexOf(M)===-1)return o.push(M),M;return Re("WebGLRenderer: Maximum number of simultaneously usable uniforms groups reached."),0}function d(M){let T=r[M.id],E=M.uniforms,A=M.__cache;n.bindBuffer(n.UNIFORM_BUFFER,T);for(let I=0,D=E.length;I<D;I++){let y=Array.isArray(E[I])?E[I]:[E[I]];for(let S=0,q=y.length;S<q;S++){let w=y[S];if(h(w,I,S,A)===!0){let U=w.__offset,V=Array.isArray(w.value)?w.value:[w.value],W=0;for(let B=0;B<V.length;B++){let H=V[B],L=x(H);typeof H=="number"||typeof H=="boolean"?(w.__data[0]=H,n.bufferSubData(n.UNIFORM_BUFFER,U+W,w.__data)):H.isMatrix3?(w.__data[0]=H.elements[0],w.__data[1]=H.elements[1],w.__data[2]=H.elements[2],w.__data[3]=0,w.__data[4]=H.elements[3],w.__data[5]=H.elements[4],w.__data[6]=H.elements[5],w.__data[7]=0,w.__data[8]=H.elements[6],w.__data[9]=H.elements[7],w.__data[10]=H.elements[8],w.__data[11]=0):(H.toArray(w.__data,W),W+=L.storage/Float32Array.BYTES_PER_ELEMENT)}n.bufferSubData(n.UNIFORM_BUFFER,U,w.__data)}}}n.bindBuffer(n.UNIFORM_BUFFER,null)}function h(M,T,E,A){let I=M.value,D=T+"_"+E;if(A[D]===void 0)return typeof I=="number"||typeof I=="boolean"?A[D]=I:A[D]=I.clone(),!0;{let y=A[D];if(typeof I=="number"||typeof I=="boolean"){if(y!==I)return A[D]=I,!0}else if(y.equals(I)===!1)return y.copy(I),!0}return!1}function g(M){let T=M.uniforms,E=0,A=16;for(let D=0,y=T.length;D<y;D++){let S=Array.isArray(T[D])?T[D]:[T[D]];for(let q=0,w=S.length;q<w;q++){let U=S[q],V=Array.isArray(U.value)?U.value:[U.value];for(let W=0,B=V.length;W<B;W++){let H=V[W],L=x(H),Q=E%A,Z=Q%L.boundary,ue=Q+Z;E+=Z,ue!==0&&A-ue<L.storage&&(E+=A-ue),U.__data=new Float32Array(L.storage/Float32Array.BYTES_PER_ELEMENT),U.__offset=E,E+=L.storage}}}let I=E%A;return I>0&&(E+=A-I),M.__size=E,M.__cache={},this}function x(M){let T={boundary:0,storage:0};return typeof M=="number"||typeof M=="boolean"?(T.boundary=4,T.storage=4):M.isVector2?(T.boundary=8,T.storage=8):M.isVector3||M.isColor?(T.boundary=16,T.storage=12):M.isVector4?(T.boundary=16,T.storage=16):M.isMatrix3?(T.boundary=48,T.storage=48):M.isMatrix4?(T.boundary=64,T.storage=64):M.isTexture?Ne("WebGLRenderer: Texture samplers can not be part of an uniforms group."):Ne("WebGLRenderer: Unsupported uniform value type.",M),T}function m(M){let T=M.target;T.removeEventListener("dispose",m);let E=o.indexOf(T.__bindingPointIndex);o.splice(E,1),n.deleteBuffer(r[T.id]),delete r[T.id],delete s[T.id]}function p(){for(let M in r)n.deleteBuffer(r[M]);o=[],r={},s={}}return{bind:c,update:l,dispose:p}}var wR=new Uint16Array([12469,15057,12620,14925,13266,14620,13807,14376,14323,13990,14545,13625,14713,13328,14840,12882,14931,12528,14996,12233,15039,11829,15066,11525,15080,11295,15085,10976,15082,10705,15073,10495,13880,14564,13898,14542,13977,14430,14158,14124,14393,13732,14556,13410,14702,12996,14814,12596,14891,12291,14937,11834,14957,11489,14958,11194,14943,10803,14921,10506,14893,10278,14858,9960,14484,14039,14487,14025,14499,13941,14524,13740,14574,13468,14654,13106,14743,12678,14818,12344,14867,11893,14889,11509,14893,11180,14881,10751,14852,10428,14812,10128,14765,9754,14712,9466,14764,13480,14764,13475,14766,13440,14766,13347,14769,13070,14786,12713,14816,12387,14844,11957,14860,11549,14868,11215,14855,10751,14825,10403,14782,10044,14729,9651,14666,9352,14599,9029,14967,12835,14966,12831,14963,12804,14954,12723,14936,12564,14917,12347,14900,11958,14886,11569,14878,11247,14859,10765,14828,10401,14784,10011,14727,9600,14660,9289,14586,8893,14508,8533,15111,12234,15110,12234,15104,12216,15092,12156,15067,12010,15028,11776,14981,11500,14942,11205,14902,10752,14861,10393,14812,9991,14752,9570,14682,9252,14603,8808,14519,8445,14431,8145,15209,11449,15208,11451,15202,11451,15190,11438,15163,11384,15117,11274,15055,10979,14994,10648,14932,10343,14871,9936,14803,9532,14729,9218,14645,8742,14556,8381,14461,8020,14365,7603,15273,10603,15272,10607,15267,10619,15256,10631,15231,10614,15182,10535,15118,10389,15042,10167,14963,9787,14883,9447,14800,9115,14710,8665,14615,8318,14514,7911,14411,7507,14279,7198,15314,9675,15313,9683,15309,9712,15298,9759,15277,9797,15229,9773,15166,9668,15084,9487,14995,9274,14898,8910,14800,8539,14697,8234,14590,7790,14479,7409,14367,7067,14178,6621,15337,8619,15337,8631,15333,8677,15325,8769,15305,8871,15264,8940,15202,8909,15119,8775,15022,8565,14916,8328,14804,8009,14688,7614,14569,7287,14448,6888,14321,6483,14088,6171,15350,7402,15350,7419,15347,7480,15340,7613,15322,7804,15287,7973,15229,8057,15148,8012,15046,7846,14933,7611,14810,7357,14682,7069,14552,6656,14421,6316,14251,5948,14007,5528,15356,5942,15356,5977,15353,6119,15348,6294,15332,6551,15302,6824,15249,7044,15171,7122,15070,7050,14949,6861,14818,6611,14679,6349,14538,6067,14398,5651,14189,5311,13935,4958,15359,4123,15359,4153,15356,4296,15353,4646,15338,5160,15311,5508,15263,5829,15188,6042,15088,6094,14966,6001,14826,5796,14678,5543,14527,5287,14377,4985,14133,4586,13869,4257,15360,1563,15360,1642,15358,2076,15354,2636,15341,3350,15317,4019,15273,4429,15203,4732,15105,4911,14981,4932,14836,4818,14679,4621,14517,4386,14359,4156,14083,3795,13808,3437,15360,122,15360,137,15358,285,15355,636,15344,1274,15322,2177,15281,2765,15215,3223,15120,3451,14995,3569,14846,3567,14681,3466,14511,3305,14344,3121,14037,2800,13753,2467,15360,0,15360,1,15359,21,15355,89,15346,253,15325,479,15287,796,15225,1148,15133,1492,15008,1749,14856,1882,14685,1886,14506,1783,14324,1608,13996,1398,13702,1183]),Ri=null;function CR(){return Ri===null&&(Ri=new Du(wR,16,16,Ts,Di),Ri.name="DFG_LUT",Ri.minFilter=tn,Ri.magFilter=tn,Ri.wrapS=Ei,Ri.wrapT=Ei,Ri.generateMipmaps=!1,Ri.needsUpdate=!0),Ri}var Bd=class{constructor(e={}){let{canvas:t=B0(),context:i=null,depth:r=!0,stencil:s=!1,alpha:o=!1,antialias:a=!1,premultipliedAlpha:c=!0,preserveDrawingBuffer:l=!1,powerPreference:u="default",failIfMajorPerformanceCaveat:f=!1,reversedDepthBuffer:d=!1,outputBufferType:h=On}=e;this.isWebGLRenderer=!0;let g;if(i!==null){if(typeof WebGLRenderingContext<"u"&&i instanceof WebGLRenderingContext)throw new Error("THREE.WebGLRenderer: WebGL 1 is not supported since r163.");g=i.getContextAttributes().alpha}else g=o;let x=h,m=new Set([ed,Qu,Ku]),p=new Set([On,fi,Io,Ao,Zu,Ju]),M=new Uint32Array(4),T=new Int32Array(4),E=null,A=null,I=[],D=[],y=null;this.domElement=t,this.debug={checkShaderErrors:!0,onShaderError:null},this.autoClear=!0,this.autoClearColor=!0,this.autoClearDepth=!0,this.autoClearStencil=!0,this.sortObjects=!0,this.clippingPlanes=[],this.localClippingEnabled=!1,this.toneMapping=di,this.toneMappingExposure=1,this.transmissionResolutionScale=1;let S=this,q=!1;this._outputColorSpace=Dn;let w=0,U=0,V=null,W=-1,B=null,H=new Pt,L=new Pt,Q=null,Z=new Xe(0),ue=0,ge=t.width,fe=t.height,He=1,Tt=null,St=null,X=new Pt(0,0,ge,fe),ne=new Pt(0,0,ge,fe),se=!1,Ve=new Va,Ie=!1,Pe=!1,Yt=new Ut,nt=new k,dt=new Pt,yt={background:null,fog:null,environment:null,overrideMaterial:null,isScene:!0},Ge=!1;function Lt(){return V===null?He:1}let C=i;function Bt(_,P){return t.getContext(_,P)}try{let _={alpha:!0,depth:r,stencil:s,antialias:a,premultipliedAlpha:c,preserveDrawingBuffer:l,powerPreference:u,failIfMajorPerformanceCaveat:f};if("setAttribute"in t&&t.setAttribute("data-engine",`three.js r${"183"}`),t.addEventListener("webglcontextlost",_e,!1),t.addEventListener("webglcontextrestored",Fe,!1),t.addEventListener("webglcontextcreationerror",Mt,!1),C===null){let P="webgl2";if(C=Bt(P,_),C===null)throw Bt(P)?new Error("Error creating WebGL context with your selected attributes."):new Error("Error creating WebGL context.")}}catch(_){throw Re("WebGLRenderer: "+_.message),_}let lt,xt,be,b,v,N,$,Y,j,ve,ie,we,De,J,ee,ye,xe,de,We,R,re,te,pe;function K(){lt=new OD(C),lt.init(),re=new xR(C,lt),xt=new CD(C,lt,e,re),be=new yR(C,lt),xt.reversedDepthBuffer&&d&&be.buffers.depth.setReversed(!0),b=new UD(C),v=new rR,N=new _R(C,lt,be,v,xt,re,b),$=new LD(S),Y=new GC(C),te=new TD(C,Y),j=new FD(C,Y,b,te),ve=new VD(C,j,Y,te,b),de=new BD(C,xt,N),ee=new ID(v),ie=new iR(S,$,lt,xt,te,ee),we=new ER(S,v),De=new oR,J=new fR(lt),xe=new ED(S,$,be,ve,g,c),ye=new vR(S,ve,xt),pe=new TR(C,b,xt,be),We=new wD(C,lt,b),R=new kD(C,lt,b),b.programs=ie.programs,S.capabilities=xt,S.extensions=lt,S.properties=v,S.renderLists=De,S.shadowMap=ye,S.state=be,S.info=b}K(),x!==On&&(y=new zD(x,t.width,t.height,r,s));let G=new Tg(S,C);this.xr=G,this.getContext=function(){return C},this.getContextAttributes=function(){return C.getContextAttributes()},this.forceContextLoss=function(){let _=lt.get("WEBGL_lose_context");_&&_.loseContext()},this.forceContextRestore=function(){let _=lt.get("WEBGL_lose_context");_&&_.restoreContext()},this.getPixelRatio=function(){return He},this.setPixelRatio=function(_){_!==void 0&&(He=_,this.setSize(ge,fe,!1))},this.getSize=function(_){return _.set(ge,fe)},this.setSize=function(_,P,z=!0){if(G.isPresenting){Ne("WebGLRenderer: Can't change size while VR device is presenting.");return}ge=_,fe=P,t.width=Math.floor(_*He),t.height=Math.floor(P*He),z===!0&&(t.style.width=_+"px",t.style.height=P+"px"),y!==null&&y.setSize(t.width,t.height),this.setViewport(0,0,_,P)},this.getDrawingBufferSize=function(_){return _.set(ge*He,fe*He).floor()},this.setDrawingBufferSize=function(_,P,z){ge=_,fe=P,He=z,t.width=Math.floor(_*z),t.height=Math.floor(P*z),this.setViewport(0,0,_,P)},this.setEffects=function(_){if(x===On){console.error("THREE.WebGLRenderer: setEffects() requires outputBufferType set to HalfFloatType or FloatType.");return}if(_){for(let P=0;P<_.length;P++)if(_[P].isOutputPass===!0){console.warn("THREE.WebGLRenderer: OutputPass is not needed in setEffects(). Tone mapping and color space conversion are applied automatically.");break}}y.setEffects(_||[])},this.getCurrentViewport=function(_){return _.copy(H)},this.getViewport=function(_){return _.copy(X)},this.setViewport=function(_,P,z,F){_.isVector4?X.set(_.x,_.y,_.z,_.w):X.set(_,P,z,F),be.viewport(H.copy(X).multiplyScalar(He).round())},this.getScissor=function(_){return _.copy(ne)},this.setScissor=function(_,P,z,F){_.isVector4?ne.set(_.x,_.y,_.z,_.w):ne.set(_,P,z,F),be.scissor(L.copy(ne).multiplyScalar(He).round())},this.getScissorTest=function(){return se},this.setScissorTest=function(_){be.setScissorTest(se=_)},this.setOpaqueSort=function(_){Tt=_},this.setTransparentSort=function(_){St=_},this.getClearColor=function(_){return _.copy(xe.getClearColor())},this.setClearColor=function(){xe.setClearColor(...arguments)},this.getClearAlpha=function(){return xe.getClearAlpha()},this.setClearAlpha=function(){xe.setClearAlpha(...arguments)},this.clear=function(_=!0,P=!0,z=!0){let F=0;if(_){let O=!1;if(V!==null){let ae=V.texture.format;O=m.has(ae)}if(O){let ae=V.texture.type,he=p.has(ae),ce=xe.getClearColor(),Me=xe.getClearAlpha(),Ee=ce.r,Ue=ce.g,je=ce.b;he?(M[0]=Ee,M[1]=Ue,M[2]=je,M[3]=Me,C.clearBufferuiv(C.COLOR,0,M)):(T[0]=Ee,T[1]=Ue,T[2]=je,T[3]=Me,C.clearBufferiv(C.COLOR,0,T))}else F|=C.COLOR_BUFFER_BIT}P&&(F|=C.DEPTH_BUFFER_BIT),z&&(F|=C.STENCIL_BUFFER_BIT,this.state.buffers.stencil.setMask(4294967295)),F!==0&&C.clear(F)},this.clearColor=function(){this.clear(!0,!1,!1)},this.clearDepth=function(){this.clear(!1,!0,!1)},this.clearStencil=function(){this.clear(!1,!1,!0)},this.dispose=function(){t.removeEventListener("webglcontextlost",_e,!1),t.removeEventListener("webglcontextrestored",Fe,!1),t.removeEventListener("webglcontextcreationerror",Mt,!1),xe.dispose(),De.dispose(),J.dispose(),v.dispose(),$.dispose(),ve.dispose(),te.dispose(),pe.dispose(),ie.dispose(),G.dispose(),G.removeEventListener("sessionstart",Cg),G.removeEventListener("sessionend",Ig),Ur.stop()};function _e(_){_.preventDefault(),og("WebGLRenderer: Context Lost."),q=!0}function Fe(){og("WebGLRenderer: Context Restored."),q=!1;let _=b.autoReset,P=ye.enabled,z=ye.autoUpdate,F=ye.needsUpdate,O=ye.type;K(),b.autoReset=_,ye.enabled=P,ye.autoUpdate=z,ye.needsUpdate=F,ye.type=O}function Mt(_){Re("WebGLRenderer: A WebGL context could not be created. Reason: ",_.statusMessage)}function ft(_){let P=_.target;P.removeEventListener("dispose",ft),Pi(P)}function Pi(_){Li(_),v.remove(_)}function Li(_){let P=v.get(_).programs;P!==void 0&&(P.forEach(function(z){ie.releaseProgram(z)}),_.isShaderMaterial&&ie.releaseShaderCache(_))}this.renderBufferDirect=function(_,P,z,F,O,ae){P===null&&(P=yt);let he=O.isMesh&&O.matrixWorld.determinant()<0,ce=_M(_,P,z,F,O);be.setMaterial(F,he);let Me=z.index,Ee=1;if(F.wireframe===!0){if(Me=j.getWireframeAttribute(z),Me===void 0)return;Ee=2}let Ue=z.drawRange,je=z.attributes.position,Te=Ue.start*Ee,pt=(Ue.start+Ue.count)*Ee;ae!==null&&(Te=Math.max(Te,ae.start*Ee),pt=Math.min(pt,(ae.start+ae.count)*Ee)),Me!==null?(Te=Math.max(Te,0),pt=Math.min(pt,Me.count)):je!=null&&(Te=Math.max(Te,0),pt=Math.min(pt,je.count));let Ot=pt-Te;if(Ot<0||Ot===1/0)return;te.setup(O,F,ce,z,Me);let Ct,mt=We;if(Me!==null&&(Ct=Y.get(Me),mt=R,mt.setIndex(Ct)),O.isMesh)F.wireframe===!0?(be.setLineWidth(F.wireframeLinewidth*Lt()),mt.setMode(C.LINES)):mt.setMode(C.TRIANGLES);else if(O.isLine){let nn=F.linewidth;nn===void 0&&(nn=1),be.setLineWidth(nn*Lt()),O.isLineSegments?mt.setMode(C.LINES):O.isLineLoop?mt.setMode(C.LINE_LOOP):mt.setMode(C.LINE_STRIP)}else O.isPoints?mt.setMode(C.POINTS):O.isSprite&&mt.setMode(C.TRIANGLES);if(O.isBatchedMesh)if(O._multiDrawInstances!==null)Na("WebGLRenderer: renderMultiDrawInstances has been deprecated and will be removed in r184. Append to renderMultiDraw arguments and use indirection."),mt.renderMultiDrawInstances(O._multiDrawStarts,O._multiDrawCounts,O._multiDrawCount,O._multiDrawInstances);else if(lt.get("WEBGL_multi_draw"))mt.renderMultiDraw(O._multiDrawStarts,O._multiDrawCounts,O._multiDrawCount);else{let nn=O._multiDrawStarts,Se=O._multiDrawCounts,Sn=O._multiDrawCount,ot=Me?Y.get(Me).bytesPerElement:1,Zn=v.get(F).currentProgram.getUniforms();for(let pi=0;pi<Sn;pi++)Zn.setValue(C,"_gl_DrawID",pi),mt.render(nn[pi]/ot,Se[pi])}else if(O.isInstancedMesh)mt.renderInstances(Te,Ot,O.count);else if(z.isInstancedBufferGeometry){let nn=z._maxInstanceCount!==void 0?z._maxInstanceCount:1/0,Se=Math.min(z.instanceCount,nn);mt.renderInstances(Te,Ot,Se)}else mt.render(Te,Ot)};function wg(_,P,z){_.transparent===!0&&_.side===Ii&&_.forceSinglePass===!1?(_.side=vn,_.needsUpdate=!0,ac(_,P,z),_.side=er,_.needsUpdate=!0,ac(_,P,z),_.side=Ii):ac(_,P,z)}this.compile=function(_,P,z=null){z===null&&(z=_),A=J.get(z),A.init(P),D.push(A),z.traverseVisible(function(O){O.isLight&&O.layers.test(P.layers)&&(A.pushLight(O),O.castShadow&&A.pushShadow(O))}),_!==z&&_.traverseVisible(function(O){O.isLight&&O.layers.test(P.layers)&&(A.pushLight(O),O.castShadow&&A.pushShadow(O))}),A.setupLights();let F=new Set;return _.traverse(function(O){if(!(O.isMesh||O.isPoints||O.isLine||O.isSprite))return;let ae=O.material;if(ae)if(Array.isArray(ae))for(let he=0;he<ae.length;he++){let ce=ae[he];wg(ce,z,O),F.add(ce)}else wg(ae,z,O),F.add(ae)}),A=D.pop(),F},this.compileAsync=function(_,P,z=null){let F=this.compile(_,P,z);return new Promise(O=>{function ae(){if(F.forEach(function(he){v.get(he).currentProgram.isReady()&&F.delete(he)}),F.size===0){O(_);return}setTimeout(ae,10)}lt.get("KHR_parallel_shader_compile")!==null?ae():setTimeout(ae,10)})};let Kd=null;function yM(_){Kd&&Kd(_)}function Cg(){Ur.stop()}function Ig(){Ur.start()}let Ur=new hM;Ur.setAnimationLoop(yM),typeof self<"u"&&Ur.setContext(self),this.setAnimationLoop=function(_){Kd=_,G.setAnimationLoop(_),_===null?Ur.stop():Ur.start()},G.addEventListener("sessionstart",Cg),G.addEventListener("sessionend",Ig),this.render=function(_,P){if(P!==void 0&&P.isCamera!==!0){Re("WebGLRenderer.render: camera is not an instance of THREE.Camera.");return}if(q===!0)return;let z=G.enabled===!0&&G.isPresenting===!0,F=y!==null&&(V===null||z)&&y.begin(S,V);if(_.matrixWorldAutoUpdate===!0&&_.updateMatrixWorld(),P.parent===null&&P.matrixWorldAutoUpdate===!0&&P.updateMatrixWorld(),G.enabled===!0&&G.isPresenting===!0&&(y===null||y.isCompositing()===!1)&&(G.cameraAutoUpdate===!0&&G.updateCamera(P),P=G.getCamera()),_.isScene===!0&&_.onBeforeRender(S,_,P,V),A=J.get(_,D.length),A.init(P),D.push(A),Yt.multiplyMatrices(P.projectionMatrix,P.matrixWorldInverse),Ve.setFromProjectionMatrix(Yt,ui,P.reversedDepth),Pe=this.localClippingEnabled,Ie=ee.init(this.clippingPlanes,Pe),E=De.get(_,I.length),E.init(),I.push(E),G.enabled===!0&&G.isPresenting===!0){let he=S.xr.getDepthSensingMesh();he!==null&&Qd(he,P,-1/0,S.sortObjects)}Qd(_,P,0,S.sortObjects),E.finish(),S.sortObjects===!0&&E.sort(Tt,St),Ge=G.enabled===!1||G.isPresenting===!1||G.hasDepthSensing()===!1,Ge&&xe.addToRenderList(E,_),this.info.render.frame++,Ie===!0&&ee.beginShadows();let O=A.state.shadowsArray;if(ye.render(O,_,P),Ie===!0&&ee.endShadows(),this.info.autoReset===!0&&this.info.reset(),(F&&y.hasRenderPass())===!1){let he=E.opaque,ce=E.transmissive;if(A.setupLights(),P.isArrayCamera){let Me=P.cameras;if(ce.length>0)for(let Ee=0,Ue=Me.length;Ee<Ue;Ee++){let je=Me[Ee];Dg(he,ce,_,je)}Ge&&xe.render(_);for(let Ee=0,Ue=Me.length;Ee<Ue;Ee++){let je=Me[Ee];Ag(E,_,je,je.viewport)}}else ce.length>0&&Dg(he,ce,_,P),Ge&&xe.render(_),Ag(E,_,P)}V!==null&&U===0&&(N.updateMultisampleRenderTarget(V),N.updateRenderTargetMipmap(V)),F&&y.end(S),_.isScene===!0&&_.onAfterRender(S,_,P),te.resetDefaultState(),W=-1,B=null,D.pop(),D.length>0?(A=D[D.length-1],Ie===!0&&ee.setGlobalState(S.clippingPlanes,A.state.camera)):A=null,I.pop(),I.length>0?E=I[I.length-1]:E=null};function Qd(_,P,z,F){if(_.visible===!1)return;if(_.layers.test(P.layers)){if(_.isGroup)z=_.renderOrder;else if(_.isLOD)_.autoUpdate===!0&&_.update(P);else if(_.isLight)A.pushLight(_),_.castShadow&&A.pushShadow(_);else if(_.isSprite){if(!_.frustumCulled||Ve.intersectsSprite(_)){F&&dt.setFromMatrixPosition(_.matrixWorld).applyMatrix4(Yt);let he=ve.update(_),ce=_.material;ce.visible&&E.push(_,he,ce,z,dt.z,null)}}else if((_.isMesh||_.isLine||_.isPoints)&&(!_.frustumCulled||Ve.intersectsObject(_))){let he=ve.update(_),ce=_.material;if(F&&(_.boundingSphere!==void 0?(_.boundingSphere===null&&_.computeBoundingSphere(),dt.copy(_.boundingSphere.center)):(he.boundingSphere===null&&he.computeBoundingSphere(),dt.copy(he.boundingSphere.center)),dt.applyMatrix4(_.matrixWorld).applyMatrix4(Yt)),Array.isArray(ce)){let Me=he.groups;for(let Ee=0,Ue=Me.length;Ee<Ue;Ee++){let je=Me[Ee],Te=ce[je.materialIndex];Te&&Te.visible&&E.push(_,he,Te,z,dt.z,je)}}else ce.visible&&E.push(_,he,ce,z,dt.z,null)}}let ae=_.children;for(let he=0,ce=ae.length;he<ce;he++)Qd(ae[he],P,z,F)}function Ag(_,P,z,F){let{opaque:O,transmissive:ae,transparent:he}=_;A.setupLightsView(z),Ie===!0&&ee.setGlobalState(S.clippingPlanes,z),F&&be.viewport(H.copy(F)),O.length>0&&oc(O,P,z),ae.length>0&&oc(ae,P,z),he.length>0&&oc(he,P,z),be.buffers.depth.setTest(!0),be.buffers.depth.setMask(!0),be.buffers.color.setMask(!0),be.setPolygonOffset(!1)}function Dg(_,P,z,F){if((z.isScene===!0?z.overrideMaterial:null)!==null)return;if(A.state.transmissionRenderTarget[F.id]===void 0){let Te=lt.has("EXT_color_buffer_half_float")||lt.has("EXT_color_buffer_float");A.state.transmissionRenderTarget[F.id]=new Rn(1,1,{generateMipmaps:!0,type:Te?Di:On,minFilter:Or,samples:Math.max(4,xt.samples),stencilBuffer:s,resolveDepthBuffer:!1,resolveStencilBuffer:!1,colorSpace:tt.workingColorSpace})}let ae=A.state.transmissionRenderTarget[F.id],he=F.viewport||H;ae.setSize(he.z*S.transmissionResolutionScale,he.w*S.transmissionResolutionScale);let ce=S.getRenderTarget(),Me=S.getActiveCubeFace(),Ee=S.getActiveMipmapLevel();S.setRenderTarget(ae),S.getClearColor(Z),ue=S.getClearAlpha(),ue<1&&S.setClearColor(16777215,.5),S.clear(),Ge&&xe.render(z);let Ue=S.toneMapping;S.toneMapping=di;let je=F.viewport;if(F.viewport!==void 0&&(F.viewport=void 0),A.setupLightsView(F),Ie===!0&&ee.setGlobalState(S.clippingPlanes,F),oc(_,z,F),N.updateMultisampleRenderTarget(ae),N.updateRenderTargetMipmap(ae),lt.has("WEBGL_multisampled_render_to_texture")===!1){let Te=!1;for(let pt=0,Ot=P.length;pt<Ot;pt++){let Ct=P[pt],{object:mt,geometry:nn,material:Se,group:Sn}=Ct;if(Se.side===Ii&&mt.layers.test(F.layers)){let ot=Se.side;Se.side=vn,Se.needsUpdate=!0,Rg(mt,z,F,nn,Se,Sn),Se.side=ot,Se.needsUpdate=!0,Te=!0}}Te===!0&&(N.updateMultisampleRenderTarget(ae),N.updateRenderTargetMipmap(ae))}S.setRenderTarget(ce,Me,Ee),S.setClearColor(Z,ue),je!==void 0&&(F.viewport=je),S.toneMapping=Ue}function oc(_,P,z){let F=P.isScene===!0?P.overrideMaterial:null;for(let O=0,ae=_.length;O<ae;O++){let he=_[O],{object:ce,geometry:Me,group:Ee}=he,Ue=he.material;Ue.allowOverride===!0&&F!==null&&(Ue=F),ce.layers.test(z.layers)&&Rg(ce,P,z,Me,Ue,Ee)}}function Rg(_,P,z,F,O,ae){_.onBeforeRender(S,P,z,F,O,ae),_.modelViewMatrix.multiplyMatrices(z.matrixWorldInverse,_.matrixWorld),_.normalMatrix.getNormalMatrix(_.modelViewMatrix),O.onBeforeRender(S,P,z,F,_,ae),O.transparent===!0&&O.side===Ii&&O.forceSinglePass===!1?(O.side=vn,O.needsUpdate=!0,S.renderBufferDirect(z,P,F,O,_,ae),O.side=er,O.needsUpdate=!0,S.renderBufferDirect(z,P,F,O,_,ae),O.side=Ii):S.renderBufferDirect(z,P,F,O,_,ae),_.onAfterRender(S,P,z,F,O,ae)}function ac(_,P,z){P.isScene!==!0&&(P=yt);let F=v.get(_),O=A.state.lights,ae=A.state.shadowsArray,he=O.state.version,ce=ie.getParameters(_,O.state,ae,P,z),Me=ie.getProgramCacheKey(ce),Ee=F.programs;F.environment=_.isMeshStandardMaterial||_.isMeshLambertMaterial||_.isMeshPhongMaterial?P.environment:null,F.fog=P.fog;let Ue=_.isMeshStandardMaterial||_.isMeshLambertMaterial&&!_.envMap||_.isMeshPhongMaterial&&!_.envMap;F.envMap=$.get(_.envMap||F.environment,Ue),F.envMapRotation=F.environment!==null&&_.envMap===null?P.environmentRotation:_.envMapRotation,Ee===void 0&&(_.addEventListener("dispose",ft),Ee=new Map,F.programs=Ee);let je=Ee.get(Me);if(je!==void 0){if(F.currentProgram===je&&F.lightsStateVersion===he)return Pg(_,ce),je}else ce.uniforms=ie.getUniforms(_),_.onBeforeCompile(ce,S),je=ie.acquireProgram(ce,Me),Ee.set(Me,je),F.uniforms=ce.uniforms;let Te=F.uniforms;return(!_.isShaderMaterial&&!_.isRawShaderMaterial||_.clipping===!0)&&(Te.clippingPlanes=ee.uniform),Pg(_,ce),F.needsLights=MM(_),F.lightsStateVersion=he,F.needsLights&&(Te.ambientLightColor.value=O.state.ambient,Te.lightProbe.value=O.state.probe,Te.directionalLights.value=O.state.directional,Te.directionalLightShadows.value=O.state.directionalShadow,Te.spotLights.value=O.state.spot,Te.spotLightShadows.value=O.state.spotShadow,Te.rectAreaLights.value=O.state.rectArea,Te.ltc_1.value=O.state.rectAreaLTC1,Te.ltc_2.value=O.state.rectAreaLTC2,Te.pointLights.value=O.state.point,Te.pointLightShadows.value=O.state.pointShadow,Te.hemisphereLights.value=O.state.hemi,Te.directionalShadowMatrix.value=O.state.directionalShadowMatrix,Te.spotLightMatrix.value=O.state.spotLightMatrix,Te.spotLightMap.value=O.state.spotLightMap,Te.pointShadowMatrix.value=O.state.pointShadowMatrix),F.currentProgram=je,F.uniformsList=null,je}function Ng(_){if(_.uniformsList===null){let P=_.currentProgram.getUniforms();_.uniformsList=Ro.seqWithValue(P.seq,_.uniforms)}return _.uniformsList}function Pg(_,P){let z=v.get(_);z.outputColorSpace=P.outputColorSpace,z.batching=P.batching,z.batchingColor=P.batchingColor,z.instancing=P.instancing,z.instancingColor=P.instancingColor,z.instancingMorph=P.instancingMorph,z.skinning=P.skinning,z.morphTargets=P.morphTargets,z.morphNormals=P.morphNormals,z.morphColors=P.morphColors,z.morphTargetsCount=P.morphTargetsCount,z.numClippingPlanes=P.numClippingPlanes,z.numIntersection=P.numClipIntersection,z.vertexAlphas=P.vertexAlphas,z.vertexTangents=P.vertexTangents,z.toneMapping=P.toneMapping}function _M(_,P,z,F,O){P.isScene!==!0&&(P=yt),N.resetTextureUnits();let ae=P.fog,he=F.isMeshStandardMaterial||F.isMeshLambertMaterial||F.isMeshPhongMaterial?P.environment:null,ce=V===null?S.outputColorSpace:V.isXRRenderTarget===!0?V.texture.colorSpace:Ms,Me=F.isMeshStandardMaterial||F.isMeshLambertMaterial&&!F.envMap||F.isMeshPhongMaterial&&!F.envMap,Ee=$.get(F.envMap||he,Me),Ue=F.vertexColors===!0&&!!z.attributes.color&&z.attributes.color.itemSize===4,je=!!z.attributes.tangent&&(!!F.normalMap||F.anisotropy>0),Te=!!z.morphAttributes.position,pt=!!z.morphAttributes.normal,Ot=!!z.morphAttributes.color,Ct=di;F.toneMapped&&(V===null||V.isXRRenderTarget===!0)&&(Ct=S.toneMapping);let mt=z.morphAttributes.position||z.morphAttributes.normal||z.morphAttributes.color,nn=mt!==void 0?mt.length:0,Se=v.get(F),Sn=A.state.lights;if(Ie===!0&&(Pe===!0||_!==B)){let Zt=_===B&&F.id===W;ee.setState(F,_,Zt)}let ot=!1;F.version===Se.__version?(Se.needsLights&&Se.lightsStateVersion!==Sn.state.version||Se.outputColorSpace!==ce||O.isBatchedMesh&&Se.batching===!1||!O.isBatchedMesh&&Se.batching===!0||O.isBatchedMesh&&Se.batchingColor===!0&&O.colorTexture===null||O.isBatchedMesh&&Se.batchingColor===!1&&O.colorTexture!==null||O.isInstancedMesh&&Se.instancing===!1||!O.isInstancedMesh&&Se.instancing===!0||O.isSkinnedMesh&&Se.skinning===!1||!O.isSkinnedMesh&&Se.skinning===!0||O.isInstancedMesh&&Se.instancingColor===!0&&O.instanceColor===null||O.isInstancedMesh&&Se.instancingColor===!1&&O.instanceColor!==null||O.isInstancedMesh&&Se.instancingMorph===!0&&O.morphTexture===null||O.isInstancedMesh&&Se.instancingMorph===!1&&O.morphTexture!==null||Se.envMap!==Ee||F.fog===!0&&Se.fog!==ae||Se.numClippingPlanes!==void 0&&(Se.numClippingPlanes!==ee.numPlanes||Se.numIntersection!==ee.numIntersection)||Se.vertexAlphas!==Ue||Se.vertexTangents!==je||Se.morphTargets!==Te||Se.morphNormals!==pt||Se.morphColors!==Ot||Se.toneMapping!==Ct||Se.morphTargetsCount!==nn)&&(ot=!0):(ot=!0,Se.__version=F.version);let Zn=Se.currentProgram;ot===!0&&(Zn=ac(F,P,O));let pi=!1,Br=!1,Ds=!1,_t=Zn.getUniforms(),Qt=Se.uniforms;if(be.useProgram(Zn.program)&&(pi=!0,Br=!0,Ds=!0),F.id!==W&&(W=F.id,Br=!0),pi||B!==_){be.buffers.depth.getReversed()&&_.reversedDepth!==!0&&(_._reversedDepth=!0,_.updateProjectionMatrix()),_t.setValue(C,"projectionMatrix",_.projectionMatrix),_t.setValue(C,"viewMatrix",_.matrixWorldInverse);let ar=_t.map.cameraPosition;ar!==void 0&&ar.setValue(C,nt.setFromMatrixPosition(_.matrixWorld)),xt.logarithmicDepthBuffer&&_t.setValue(C,"logDepthBufFC",2/(Math.log(_.far+1)/Math.LN2)),(F.isMeshPhongMaterial||F.isMeshToonMaterial||F.isMeshLambertMaterial||F.isMeshBasicMaterial||F.isMeshStandardMaterial||F.isShaderMaterial)&&_t.setValue(C,"isOrthographic",_.isOrthographicCamera===!0),B!==_&&(B=_,Br=!0,Ds=!0)}if(Se.needsLights&&(Sn.state.directionalShadowMap.length>0&&_t.setValue(C,"directionalShadowMap",Sn.state.directionalShadowMap,N),Sn.state.spotShadowMap.length>0&&_t.setValue(C,"spotShadowMap",Sn.state.spotShadowMap,N),Sn.state.pointShadowMap.length>0&&_t.setValue(C,"pointShadowMap",Sn.state.pointShadowMap,N)),O.isSkinnedMesh){_t.setOptional(C,O,"bindMatrix"),_t.setOptional(C,O,"bindMatrixInverse");let Zt=O.skeleton;Zt&&(Zt.boneTexture===null&&Zt.computeBoneTexture(),_t.setValue(C,"boneTexture",Zt.boneTexture,N))}O.isBatchedMesh&&(_t.setOptional(C,O,"batchingTexture"),_t.setValue(C,"batchingTexture",O._matricesTexture,N),_t.setOptional(C,O,"batchingIdTexture"),_t.setValue(C,"batchingIdTexture",O._indirectTexture,N),_t.setOptional(C,O,"batchingColorTexture"),O._colorsTexture!==null&&_t.setValue(C,"batchingColorTexture",O._colorsTexture,N));let or=z.morphAttributes;if((or.position!==void 0||or.normal!==void 0||or.color!==void 0)&&de.update(O,z,Zn),(Br||Se.receiveShadow!==O.receiveShadow)&&(Se.receiveShadow=O.receiveShadow,_t.setValue(C,"receiveShadow",O.receiveShadow)),(F.isMeshStandardMaterial||F.isMeshLambertMaterial||F.isMeshPhongMaterial)&&F.envMap===null&&P.environment!==null&&(Qt.envMapIntensity.value=P.environmentIntensity),Qt.dfgLUT!==void 0&&(Qt.dfgLUT.value=CR()),Br&&(_t.setValue(C,"toneMappingExposure",S.toneMappingExposure),Se.needsLights&&xM(Qt,Ds),ae&&F.fog===!0&&we.refreshFogUniforms(Qt,ae),we.refreshMaterialUniforms(Qt,F,He,fe,A.state.transmissionRenderTarget[_.id]),Ro.upload(C,Ng(Se),Qt,N)),F.isShaderMaterial&&F.uniformsNeedUpdate===!0&&(Ro.upload(C,Ng(Se),Qt,N),F.uniformsNeedUpdate=!1),F.isSpriteMaterial&&_t.setValue(C,"center",O.center),_t.setValue(C,"modelViewMatrix",O.modelViewMatrix),_t.setValue(C,"normalMatrix",O.normalMatrix),_t.setValue(C,"modelMatrix",O.matrixWorld),F.isShaderMaterial||F.isRawShaderMaterial){let Zt=F.uniformsGroups;for(let ar=0,Rs=Zt.length;ar<Rs;ar++){let Lg=Zt[ar];pe.update(Lg,Zn),pe.bind(Lg,Zn)}}return Zn}function xM(_,P){_.ambientLightColor.needsUpdate=P,_.lightProbe.needsUpdate=P,_.directionalLights.needsUpdate=P,_.directionalLightShadows.needsUpdate=P,_.pointLights.needsUpdate=P,_.pointLightShadows.needsUpdate=P,_.spotLights.needsUpdate=P,_.spotLightShadows.needsUpdate=P,_.rectAreaLights.needsUpdate=P,_.hemisphereLights.needsUpdate=P}function MM(_){return _.isMeshLambertMaterial||_.isMeshToonMaterial||_.isMeshPhongMaterial||_.isMeshStandardMaterial||_.isShadowMaterial||_.isShaderMaterial&&_.lights===!0}this.getActiveCubeFace=function(){return w},this.getActiveMipmapLevel=function(){return U},this.getRenderTarget=function(){return V},this.setRenderTargetTextures=function(_,P,z){let F=v.get(_);F.__autoAllocateDepthBuffer=_.resolveDepthBuffer===!1,F.__autoAllocateDepthBuffer===!1&&(F.__useRenderToTexture=!1),v.get(_.texture).__webglTexture=P,v.get(_.depthTexture).__webglTexture=F.__autoAllocateDepthBuffer?void 0:z,F.__hasExternalTextures=!0},this.setRenderTargetFramebuffer=function(_,P){let z=v.get(_);z.__webglFramebuffer=P,z.__useDefaultFramebuffer=P===void 0};let bM=C.createFramebuffer();this.setRenderTarget=function(_,P=0,z=0){V=_,w=P,U=z;let F=null,O=!1,ae=!1;if(_){let ce=v.get(_);if(ce.__useDefaultFramebuffer!==void 0){be.bindFramebuffer(C.FRAMEBUFFER,ce.__webglFramebuffer),H.copy(_.viewport),L.copy(_.scissor),Q=_.scissorTest,be.viewport(H),be.scissor(L),be.setScissorTest(Q),W=-1;return}else if(ce.__webglFramebuffer===void 0)N.setupRenderTarget(_);else if(ce.__hasExternalTextures)N.rebindTextures(_,v.get(_.texture).__webglTexture,v.get(_.depthTexture).__webglTexture);else if(_.depthBuffer){let Ue=_.depthTexture;if(ce.__boundDepthTexture!==Ue){if(Ue!==null&&v.has(Ue)&&(_.width!==Ue.image.width||_.height!==Ue.image.height))throw new Error("WebGLRenderTarget: Attached DepthTexture is initialized to the incorrect size.");N.setupDepthRenderbuffer(_)}}let Me=_.texture;(Me.isData3DTexture||Me.isDataArrayTexture||Me.isCompressedArrayTexture)&&(ae=!0);let Ee=v.get(_).__webglFramebuffer;_.isWebGLCubeRenderTarget?(Array.isArray(Ee[P])?F=Ee[P][z]:F=Ee[P],O=!0):_.samples>0&&N.useMultisampledRTT(_)===!1?F=v.get(_).__webglMultisampledFramebuffer:Array.isArray(Ee)?F=Ee[z]:F=Ee,H.copy(_.viewport),L.copy(_.scissor),Q=_.scissorTest}else H.copy(X).multiplyScalar(He).floor(),L.copy(ne).multiplyScalar(He).floor(),Q=se;if(z!==0&&(F=bM),be.bindFramebuffer(C.FRAMEBUFFER,F)&&be.drawBuffers(_,F),be.viewport(H),be.scissor(L),be.setScissorTest(Q),O){let ce=v.get(_.texture);C.framebufferTexture2D(C.FRAMEBUFFER,C.COLOR_ATTACHMENT0,C.TEXTURE_CUBE_MAP_POSITIVE_X+P,ce.__webglTexture,z)}else if(ae){let ce=P;for(let Me=0;Me<_.textures.length;Me++){let Ee=v.get(_.textures[Me]);C.framebufferTextureLayer(C.FRAMEBUFFER,C.COLOR_ATTACHMENT0+Me,Ee.__webglTexture,z,ce)}}else if(_!==null&&z!==0){let ce=v.get(_.texture);C.framebufferTexture2D(C.FRAMEBUFFER,C.COLOR_ATTACHMENT0,C.TEXTURE_2D,ce.__webglTexture,z)}W=-1},this.readRenderTargetPixels=function(_,P,z,F,O,ae,he,ce=0){if(!(_&&_.isWebGLRenderTarget)){Re("WebGLRenderer.readRenderTargetPixels: renderTarget is not THREE.WebGLRenderTarget.");return}let Me=v.get(_).__webglFramebuffer;if(_.isWebGLCubeRenderTarget&&he!==void 0&&(Me=Me[he]),Me){be.bindFramebuffer(C.FRAMEBUFFER,Me);try{let Ee=_.textures[ce],Ue=Ee.format,je=Ee.type;if(_.textures.length>1&&C.readBuffer(C.COLOR_ATTACHMENT0+ce),!xt.textureFormatReadable(Ue)){Re("WebGLRenderer.readRenderTargetPixels: renderTarget is not in RGBA or implementation defined format.");return}if(!xt.textureTypeReadable(je)){Re("WebGLRenderer.readRenderTargetPixels: renderTarget is not in UnsignedByteType or implementation defined type.");return}P>=0&&P<=_.width-F&&z>=0&&z<=_.height-O&&C.readPixels(P,z,F,O,re.convert(Ue),re.convert(je),ae)}finally{let Ee=V!==null?v.get(V).__webglFramebuffer:null;be.bindFramebuffer(C.FRAMEBUFFER,Ee)}}},this.readRenderTargetPixelsAsync=async function(_,P,z,F,O,ae,he,ce=0){if(!(_&&_.isWebGLRenderTarget))throw new Error("THREE.WebGLRenderer.readRenderTargetPixels: renderTarget is not THREE.WebGLRenderTarget.");let Me=v.get(_).__webglFramebuffer;if(_.isWebGLCubeRenderTarget&&he!==void 0&&(Me=Me[he]),Me)if(P>=0&&P<=_.width-F&&z>=0&&z<=_.height-O){be.bindFramebuffer(C.FRAMEBUFFER,Me);let Ee=_.textures[ce],Ue=Ee.format,je=Ee.type;if(_.textures.length>1&&C.readBuffer(C.COLOR_ATTACHMENT0+ce),!xt.textureFormatReadable(Ue))throw new Error("THREE.WebGLRenderer.readRenderTargetPixelsAsync: renderTarget is not in RGBA or implementation defined format.");if(!xt.textureTypeReadable(je))throw new Error("THREE.WebGLRenderer.readRenderTargetPixelsAsync: renderTarget is not in UnsignedByteType or implementation defined type.");let Te=C.createBuffer();C.bindBuffer(C.PIXEL_PACK_BUFFER,Te),C.bufferData(C.PIXEL_PACK_BUFFER,ae.byteLength,C.STREAM_READ),C.readPixels(P,z,F,O,re.convert(Ue),re.convert(je),0);let pt=V!==null?v.get(V).__webglFramebuffer:null;be.bindFramebuffer(C.FRAMEBUFFER,pt);let Ot=C.fenceSync(C.SYNC_GPU_COMMANDS_COMPLETE,0);return C.flush(),await H0(C,Ot,4),C.bindBuffer(C.PIXEL_PACK_BUFFER,Te),C.getBufferSubData(C.PIXEL_PACK_BUFFER,0,ae),C.deleteBuffer(Te),C.deleteSync(Ot),ae}else throw new Error("THREE.WebGLRenderer.readRenderTargetPixelsAsync: requested read bounds are out of range.")},this.copyFramebufferToTexture=function(_,P=null,z=0){let F=Math.pow(2,-z),O=Math.floor(_.image.width*F),ae=Math.floor(_.image.height*F),he=P!==null?P.x:0,ce=P!==null?P.y:0;N.setTexture2D(_,0),C.copyTexSubImage2D(C.TEXTURE_2D,z,0,0,he,ce,O,ae),be.unbindTexture()};let SM=C.createFramebuffer(),EM=C.createFramebuffer();this.copyTextureToTexture=function(_,P,z=null,F=null,O=0,ae=0){let he,ce,Me,Ee,Ue,je,Te,pt,Ot,Ct=_.isCompressedTexture?_.mipmaps[ae]:_.image;if(z!==null)he=z.max.x-z.min.x,ce=z.max.y-z.min.y,Me=z.isBox3?z.max.z-z.min.z:1,Ee=z.min.x,Ue=z.min.y,je=z.isBox3?z.min.z:0;else{let Qt=Math.pow(2,-O);he=Math.floor(Ct.width*Qt),ce=Math.floor(Ct.height*Qt),_.isDataArrayTexture?Me=Ct.depth:_.isData3DTexture?Me=Math.floor(Ct.depth*Qt):Me=1,Ee=0,Ue=0,je=0}F!==null?(Te=F.x,pt=F.y,Ot=F.z):(Te=0,pt=0,Ot=0);let mt=re.convert(P.format),nn=re.convert(P.type),Se;P.isData3DTexture?(N.setTexture3D(P,0),Se=C.TEXTURE_3D):P.isDataArrayTexture||P.isCompressedArrayTexture?(N.setTexture2DArray(P,0),Se=C.TEXTURE_2D_ARRAY):(N.setTexture2D(P,0),Se=C.TEXTURE_2D),C.pixelStorei(C.UNPACK_FLIP_Y_WEBGL,P.flipY),C.pixelStorei(C.UNPACK_PREMULTIPLY_ALPHA_WEBGL,P.premultiplyAlpha),C.pixelStorei(C.UNPACK_ALIGNMENT,P.unpackAlignment);let Sn=C.getParameter(C.UNPACK_ROW_LENGTH),ot=C.getParameter(C.UNPACK_IMAGE_HEIGHT),Zn=C.getParameter(C.UNPACK_SKIP_PIXELS),pi=C.getParameter(C.UNPACK_SKIP_ROWS),Br=C.getParameter(C.UNPACK_SKIP_IMAGES);C.pixelStorei(C.UNPACK_ROW_LENGTH,Ct.width),C.pixelStorei(C.UNPACK_IMAGE_HEIGHT,Ct.height),C.pixelStorei(C.UNPACK_SKIP_PIXELS,Ee),C.pixelStorei(C.UNPACK_SKIP_ROWS,Ue),C.pixelStorei(C.UNPACK_SKIP_IMAGES,je);let Ds=_.isDataArrayTexture||_.isData3DTexture,_t=P.isDataArrayTexture||P.isData3DTexture;if(_.isDepthTexture){let Qt=v.get(_),or=v.get(P),Zt=v.get(Qt.__renderTarget),ar=v.get(or.__renderTarget);be.bindFramebuffer(C.READ_FRAMEBUFFER,Zt.__webglFramebuffer),be.bindFramebuffer(C.DRAW_FRAMEBUFFER,ar.__webglFramebuffer);for(let Rs=0;Rs<Me;Rs++)Ds&&(C.framebufferTextureLayer(C.READ_FRAMEBUFFER,C.COLOR_ATTACHMENT0,v.get(_).__webglTexture,O,je+Rs),C.framebufferTextureLayer(C.DRAW_FRAMEBUFFER,C.COLOR_ATTACHMENT0,v.get(P).__webglTexture,ae,Ot+Rs)),C.blitFramebuffer(Ee,Ue,he,ce,Te,pt,he,ce,C.DEPTH_BUFFER_BIT,C.NEAREST);be.bindFramebuffer(C.READ_FRAMEBUFFER,null),be.bindFramebuffer(C.DRAW_FRAMEBUFFER,null)}else if(O!==0||_.isRenderTargetTexture||v.has(_)){let Qt=v.get(_),or=v.get(P);be.bindFramebuffer(C.READ_FRAMEBUFFER,SM),be.bindFramebuffer(C.DRAW_FRAMEBUFFER,EM);for(let Zt=0;Zt<Me;Zt++)Ds?C.framebufferTextureLayer(C.READ_FRAMEBUFFER,C.COLOR_ATTACHMENT0,Qt.__webglTexture,O,je+Zt):C.framebufferTexture2D(C.READ_FRAMEBUFFER,C.COLOR_ATTACHMENT0,C.TEXTURE_2D,Qt.__webglTexture,O),_t?C.framebufferTextureLayer(C.DRAW_FRAMEBUFFER,C.COLOR_ATTACHMENT0,or.__webglTexture,ae,Ot+Zt):C.framebufferTexture2D(C.DRAW_FRAMEBUFFER,C.COLOR_ATTACHMENT0,C.TEXTURE_2D,or.__webglTexture,ae),O!==0?C.blitFramebuffer(Ee,Ue,he,ce,Te,pt,he,ce,C.COLOR_BUFFER_BIT,C.NEAREST):_t?C.copyTexSubImage3D(Se,ae,Te,pt,Ot+Zt,Ee,Ue,he,ce):C.copyTexSubImage2D(Se,ae,Te,pt,Ee,Ue,he,ce);be.bindFramebuffer(C.READ_FRAMEBUFFER,null),be.bindFramebuffer(C.DRAW_FRAMEBUFFER,null)}else _t?_.isDataTexture||_.isData3DTexture?C.texSubImage3D(Se,ae,Te,pt,Ot,he,ce,Me,mt,nn,Ct.data):P.isCompressedArrayTexture?C.compressedTexSubImage3D(Se,ae,Te,pt,Ot,he,ce,Me,mt,Ct.data):C.texSubImage3D(Se,ae,Te,pt,Ot,he,ce,Me,mt,nn,Ct):_.isDataTexture?C.texSubImage2D(C.TEXTURE_2D,ae,Te,pt,he,ce,mt,nn,Ct.data):_.isCompressedTexture?C.compressedTexSubImage2D(C.TEXTURE_2D,ae,Te,pt,Ct.width,Ct.height,mt,Ct.data):C.texSubImage2D(C.TEXTURE_2D,ae,Te,pt,he,ce,mt,nn,Ct);C.pixelStorei(C.UNPACK_ROW_LENGTH,Sn),C.pixelStorei(C.UNPACK_IMAGE_HEIGHT,ot),C.pixelStorei(C.UNPACK_SKIP_PIXELS,Zn),C.pixelStorei(C.UNPACK_SKIP_ROWS,pi),C.pixelStorei(C.UNPACK_SKIP_IMAGES,Br),ae===0&&P.generateMipmaps&&C.generateMipmap(Se),be.unbindTexture()},this.initRenderTarget=function(_){v.get(_).__webglFramebuffer===void 0&&N.setupRenderTarget(_)},this.initTexture=function(_){_.isCubeTexture?N.setTextureCube(_,0):_.isData3DTexture?N.setTexture3D(_,0):_.isDataArrayTexture||_.isCompressedArrayTexture?N.setTexture2DArray(_,0):N.setTexture2D(_,0),be.unbindTexture()},this.resetState=function(){w=0,U=0,V=null,be.reset(),te.reset()},typeof __THREE_DEVTOOLS__<"u"&&__THREE_DEVTOOLS__.dispatchEvent(new CustomEvent("observe",{detail:this}))}get coordinateSystem(){return ui}get outputColorSpace(){return this._outputColorSpace}set outputColorSpace(e){this._outputColorSpace=e;let t=this.getContext();t.drawingBufferColorSpace=tt._getDrawingBufferColorSpace(e),t.unpackColorSpace=tt._getUnpackColorSpace()}};var AR=["heroCanvas"];function DR(n,e){n&1&&Vt(0,"canvas",3,0)}var zd=class n{heroCanvas;i18n=me(Dt);content=this.i18n.content;cvPath=ao.cvPath;reducedMotion=an(!1);supportsWebgl=an(!0);showCanvas=xr(()=>!this.reducedMotion()&&this.supportsWebgl());renderer;scene;camera;points;frameId;ngAfterViewInit(){let e=typeof window<"u"&&typeof window.matchMedia=="function";if(this.reducedMotion.set(e?window.matchMedia("(prefers-reduced-motion: reduce)").matches:!1),this.reducedMotion())return;let t=this.heroCanvas?.nativeElement;if(t)try{this.initScene(t),this.animate(),window.addEventListener("resize",this.onResize)}catch{this.supportsWebgl.set(!1),this.destroyScene()}}ngOnDestroy(){window.removeEventListener("resize",this.onResize),this.destroyScene()}initScene(e){let{width:t,height:i}=e.getBoundingClientRect();this.scene=new Oa,this.camera=new fn(50,t/Math.max(i,1),.1,100),this.camera.position.set(0,0,6),this.renderer=new Bd({canvas:e,antialias:!0,alpha:!0,powerPreference:"high-performance"}),this.renderer.setPixelRatio(Math.min(window.devicePixelRatio||1,1.8)),this.renderer.setSize(t,i,!1);let r=new Nn,s=t<768?420:720,o=new Float32Array(s*3),a=new Float32Array(s*3),c=[new Xe(5231045),new Xe(6333946),new Xe(16020150),new Xe(16096779),new Xe(3462041)];for(let f=0;f<s;f+=1){let d=f*3;o[d]=(Math.random()-.5)*10,o[d+1]=(Math.random()-.5)*6,o[d+2]=(Math.random()-.5)*4;let h=c[Math.floor(Math.random()*c.length)].clone();h.offsetHSL((Math.random()-.5)*.045,.06,.04),a[d]=h.r,a[d+1]=h.g,a[d+2]=h.b}r.setAttribute("position",new hn(o,3)),r.setAttribute("color",new hn(a,3));let l=new To({vertexColors:!0,size:t<768?.05:.038,transparent:!0,opacity:.72,depthWrite:!1,blending:tr});this.points=new Ha(r,l),this.scene.add(this.points);let u=new Xa(16777215,.42);this.scene.add(u)}animate=()=>{!this.renderer||!this.scene||!this.camera||!this.points||(this.points.rotation.y+=45e-5,this.points.rotation.x+=15e-5,this.renderer.render(this.scene,this.camera),this.frameId=window.requestAnimationFrame(this.animate))};onResize=()=>{if(!this.renderer||!this.camera||!this.heroCanvas)return;let e=this.heroCanvas.nativeElement,{width:t,height:i}=e.getBoundingClientRect();this.renderer.setSize(t,i,!1),this.camera.aspect=t/Math.max(i,1),this.camera.updateProjectionMatrix()};destroyScene(){this.frameId&&window.cancelAnimationFrame(this.frameId),this.points?.geometry.dispose(),this.points?.material instanceof Ci&&this.points.material.dispose(),this.renderer?.dispose(),this.points=void 0,this.renderer=void 0,this.scene=void 0,this.camera=void 0,this.frameId=void 0}static \u0275fac=function(t){return new(t||n)};static \u0275cmp=Et({type:n,selectors:[["app-hero"]],viewQuery:function(t,i){if(t&1&&Ol(AR,5),t&2){let r;qp(r=Xp())&&(i.heroCanvas=r.first)}},decls:18,vars:20,consts:[["heroCanvas",""],["id","hero","aria-labelledby","hero-title",1,"hero"],["aria-hidden","true",1,"hero-bg"],[1,"hero-canvas"],[1,"hero-gradient"],[1,"container","hero-content"],[1,"hero-badge",3,"innerHTML"],["id","hero-title",3,"innerHTML"],[1,"hero-subtitle",3,"innerHTML"],[1,"hero-actions"],["href","#contact",1,"btn","btn-primary",3,"innerHTML"],["href","#projects",1,"btn","btn-ghost",3,"innerHTML"],["target","_blank","rel","noopener noreferrer",1,"btn","btn-ghost",3,"href","innerHTML"]],template:function(t,i){t&1&&(kt(0,"section",1)(1,"div",2),si(2,DR,2,0,"canvas",3),Vt(3,"div",4),$t(),kt(4,"div",5),Vt(5,"p",6),Qe(6,"richText"),Vt(7,"h1",7),Qe(8,"richText"),Vt(9,"p",8),Qe(10,"richText"),kt(11,"div",9),Vt(12,"a",10),Qe(13,"richText"),Vt(14,"a",11),Qe(15,"richText"),Vt(16,"a",12),Qe(17,"richText"),$t()()()),t&2&&(le(2),oi(i.showCanvas()?2:-1),le(3),en("innerHTML",et(6,8,i.content().hero.badge),Je),le(2),en("innerHTML",et(8,10,i.content().hero.title),Je),le(2),en("innerHTML",et(10,12,i.content().hero.subtitle),Je),le(3),en("innerHTML",et(13,14,i.content().hero.ctaContact),Je),le(2),en("innerHTML",et(15,16,i.content().hero.ctaProjects),Je),le(2),en("href",i.cvPath,qi)("innerHTML",et(17,18,i.content().hero.ctaCv),Je))},dependencies:[Rt],styles:[".hero[_ngcontent-%COMP%]{position:relative;overflow:clip;min-height:min(88vh,56rem);display:grid;align-items:center;padding-block:clamp(5rem,10vw,7.5rem) clamp(3.5rem,8vw,6rem)}.hero-bg[_ngcontent-%COMP%], .hero-canvas[_ngcontent-%COMP%], .hero-gradient[_ngcontent-%COMP%]{position:absolute;inset:0}.hero-canvas[_ngcontent-%COMP%]{width:100%;height:100%;opacity:1}.hero-gradient[_ngcontent-%COMP%]{background:radial-gradient(circle at 18% 18%,color-mix(in srgb,var(--accent) 12%,transparent),transparent 52%),linear-gradient(180deg,color-mix(in srgb,var(--surface-primary) 6%,transparent),var(--surface-primary))}.hero-content[_ngcontent-%COMP%]{position:relative;z-index:2;max-width:56rem}.hero-badge[_ngcontent-%COMP%]{display:inline-block;margin:0 0 1rem;padding:.35rem .72rem;border:1px solid var(--border-color);border-radius:999px;color:var(--text-secondary);font-size:1.5rem;letter-spacing:.08em;text-transform:uppercase;font-weight:700}h1[_ngcontent-%COMP%]{margin:0;font-family:var(--font-display);font-size:clamp(2rem,7vw,4rem);line-height:1.04;letter-spacing:-.03em;color:var(--text-primary)}.hero-subtitle[_ngcontent-%COMP%]{margin:1.2rem 0 0;max-width:52ch;color:var(--text-secondary);font-size:clamp(1rem,2.6vw,1.2rem)}.hero-actions[_ngcontent-%COMP%]{margin-top:2rem;display:flex;flex-wrap:wrap;gap:.7rem}"]})};var Mn=class n{title=ga.required();id=ga();static \u0275fac=function(t){return new(t||n)};static \u0275cmp=Et({type:n,selectors:[["app-section-title"]],inputs:{title:[1,"title"],id:[1,"id"]},decls:2,vars:4,consts:[[1,"section-title",3,"id","innerHTML"]],template:function(t,i){t&1&&(Vt(0,"h2",0),Qe(1,"richText")),t&2&&en("id",i.id())("innerHTML",et(1,2,i.title()),Je)},dependencies:[Rt],styles:[".section-title[_ngcontent-%COMP%]{margin:0 0 1.25rem;font-family:var(--font-display);font-size:clamp(1.4rem,3vw,2rem);line-height:1.15;letter-spacing:-.02em;color:var(--text-primary)}"]})};var bn=class n{element=me(us);renderer=me(Up);observer;ngOnInit(){if(typeof window<"u"&&typeof window.matchMedia=="function"&&window.matchMedia("(prefers-reduced-motion: reduce)").matches){this.renderer.addClass(this.element.nativeElement,"is-visible");return}if(typeof IntersectionObserver>"u"){this.renderer.addClass(this.element.nativeElement,"is-visible");return}this.observer=new IntersectionObserver(t=>{t.forEach(i=>{i.isIntersecting&&(this.renderer.addClass(i.target,"is-visible"),this.observer?.unobserve(i.target))})},{threshold:.16}),this.observer.observe(this.element.nativeElement)}ngOnDestroy(){this.observer?.disconnect()}static \u0275fac=function(t){return new(t||n)};static \u0275dir=Hp({type:n,selectors:[["","appRevealOnScroll",""]]})};function RR(n,e){if(n&1&&(st(0,"p",5),Qe(1,"richText")),n&2){let t=e.$implicit;Ke("innerHTML",et(1,1,t),Je)}}var Gd=class n{content=me(Dt).content;static \u0275fac=function(t){return new(t||n)};static \u0275cmp=Et({type:n,selectors:[["app-about"]],decls:8,vars:4,consts:[["id","about","aria-labelledby","about-title",1,"section"],["appRevealOnScroll","",1,"container"],["id","about-section-title",3,"title"],[1,"surface","about-surface"],["id","about-title",3,"innerHTML"],[3,"innerHTML"]],template:function(t,i){t&1&&(Oe(0,"section",0)(1,"div",1),st(2,"app-section-title",2),Oe(3,"div",3),st(4,"h3",4),Qe(5,"richText"),cn(6,RR,2,3,"p",5,fs),ke()()()),t&2&&(le(2),Ke("title",i.content().sectionTitles.about),le(2),Ke("innerHTML",et(5,2,i.content().about.title),Je),le(2),ln(i.content().about.paragraphs))},dependencies:[Mn,bn,Rt],styles:[".about-surface[_ngcontent-%COMP%]   h3[_ngcontent-%COMP%]{margin:0 0 1rem;font-size:1.15rem;color:var(--text-primary)}.about-surface[_ngcontent-%COMP%]   p[_ngcontent-%COMP%]{margin:0;color:var(--text-secondary)}.about-surface[_ngcontent-%COMP%]   p[_ngcontent-%COMP%] + p[_ngcontent-%COMP%]{margin-top:.9rem}"]})};function NR(n,e){if(n&1&&(Vt(0,"li",1),Qe(1,"richText")),n&2){let t=e.$implicit;en("innerHTML",et(1,1,t),Je)}}var Po=class n{items=ga.required();static \u0275fac=function(t){return new(t||n)};static \u0275cmp=Et({type:n,selectors:[["app-chip-list"]],inputs:{items:[1,"items"]},decls:3,vars:0,consts:[[1,"chip-list"],[1,"chip",3,"innerHTML"]],template:function(t,i){t&1&&(kt(0,"ul",0),cn(1,NR,2,3,"li",1,fs),$t()),t&2&&(le(),ln(i.items()))},dependencies:[Rt],styles:[".chip-list[_ngcontent-%COMP%]{margin:0;padding:0;list-style:none;display:flex;flex-wrap:wrap;gap:.5rem}.chip[_ngcontent-%COMP%]{padding:.35rem .7rem;border:1px solid var(--border-color);border-radius:.5rem;background:color-mix(in srgb,var(--surface-secondary) 60%,transparent);color:var(--text-secondary);font-size:.82rem;letter-spacing:.02em}"]})};var PR=(n,e)=>e.title;function LR(n,e){if(n&1&&(Oe(0,"a",9),bt(1,"Demo"),ke()),n&2){let t=Gn().$implicit;Ke("href",t.links.demo,qi)}}function OR(n,e){if(n&1&&(Oe(0,"a",9),bt(1,"Repository"),ke()),n&2){let t=Gn().$implicit;Ke("href",t.links.repository,qi)}}function FR(n,e){if(n&1&&(Oe(0,"article",5),st(1,"h3",6),Qe(2,"richText"),st(3,"p",6),Qe(4,"richText"),st(5,"app-chip-list",7),Oe(6,"div",8),si(7,LR,2,1,"a",9),si(8,OR,2,1,"a",9),ke()()),n&2){let t=e.$implicit;le(),Ke("innerHTML",et(2,5,t.title),Je),le(2),Ke("innerHTML",et(4,7,t.summary),Je),le(2),Ke("items",t.stack),le(2),oi(t.links.demo?7:-1),le(),oi(t.links.repository?8:-1)}}var Wd=class n{content=me(Dt).content;static \u0275fac=function(t){return new(t||n)};static \u0275cmp=Et({type:n,selectors:[["app-projects"]],decls:7,vars:1,consts:[["id","projects","aria-labelledby","projects-title",1,"section"],[1,"container"],["appRevealOnScroll",""],["id","projects-title",3,"title"],[1,"projects-grid"],["appRevealOnScroll","",1,"surface","project-card"],[3,"innerHTML"],[3,"items"],[1,"links"],["target","_blank","rel","noopener noreferrer",3,"href"]],template:function(t,i){t&1&&(Oe(0,"section",0)(1,"div",1)(2,"div",2),st(3,"app-section-title",3),ke(),Oe(4,"div",4),cn(5,FR,9,9,"article",5,PR),ke()()()),t&2&&(le(3),Ke("title",i.content().sectionTitles.projects),le(2),ln(i.content().projects))},dependencies:[Mn,Po,bn,Rt],styles:[".projects-grid[_ngcontent-%COMP%]{display:grid;grid-template-columns:repeat(1,minmax(0,1fr));gap:1rem}.project-card[_ngcontent-%COMP%]{display:grid;gap:.95rem;transition:transform .2s ease,border-color .2s ease}.project-card[_ngcontent-%COMP%]   h3[_ngcontent-%COMP%]{margin:0;font-size:1.1rem}.project-card[_ngcontent-%COMP%]   p[_ngcontent-%COMP%]{margin:0;color:var(--text-secondary)}.project-card[_ngcontent-%COMP%]:hover{border-color:color-mix(in srgb,var(--accent) 35%,var(--border-color));transform:translateY(-2px)}.links[_ngcontent-%COMP%]{display:flex;gap:.8rem}.links[_ngcontent-%COMP%]   a[_ngcontent-%COMP%]{color:var(--text-primary);font-size:.9rem;text-decoration:underline;text-underline-offset:.2em}@media(min-width:768px){.projects-grid[_ngcontent-%COMP%]{grid-template-columns:repeat(2,minmax(0,1fr))}}"]})};var kR=(n,e)=>e.category;function UR(n,e){if(n&1&&(Oe(0,"article",5)(1,"h3"),bt(2),ke(),st(3,"app-chip-list",6),ke()),n&2){let t=e.$implicit;le(2),Wn(t.category),le(),Ke("items",t.items)}}var jd=class n{content=me(Dt).content;static \u0275fac=function(t){return new(t||n)};static \u0275cmp=Et({type:n,selectors:[["app-skills"]],decls:7,vars:1,consts:[["id","skills","aria-labelledby","skills-title",1,"section"],[1,"container"],["appRevealOnScroll",""],["id","skills-title",3,"title"],[1,"skills-grid"],["appRevealOnScroll","",1,"surface","skill-group"],[3,"items"]],template:function(t,i){t&1&&(Oe(0,"section",0)(1,"div",1)(2,"div",2),st(3,"app-section-title",3),ke(),Oe(4,"div",4),cn(5,UR,4,2,"article",5,kR),ke()()()),t&2&&(le(3),Ke("title",i.content().sectionTitles.skills),le(2),ln(i.content().skills))},dependencies:[Mn,Po,bn],styles:[".skills-grid[_ngcontent-%COMP%]{display:grid;grid-template-columns:repeat(1,minmax(0,1fr));gap:1rem}.skill-group[_ngcontent-%COMP%]{display:grid;gap:.95rem}.skill-group[_ngcontent-%COMP%]   h3[_ngcontent-%COMP%]{margin:0;font-size:1rem;color:var(--text-primary);letter-spacing:.02em}@media(min-width:768px){.skills-grid[_ngcontent-%COMP%]{grid-template-columns:repeat(3,minmax(0,1fr))}}"]})};var BR=(n,e)=>e.role+e.period;function VR(n,e){if(n&1&&(st(0,"li",6),Qe(1,"richText")),n&2){let t=e.$implicit;Ke("innerHTML",et(1,1,t),Je)}}function HR(n,e){if(n&1&&(Oe(0,"article",5)(1,"header"),st(2,"h3",6),Qe(3,"richText"),st(4,"p",6),Qe(5,"richText"),st(6,"span",6),Qe(7,"richText"),ke(),Oe(8,"ul"),cn(9,VR,2,3,"li",6,fs),ke()()),n&2){let t=e.$implicit;le(2),Ke("innerHTML",et(3,3,t.role),Je),le(2),Ke("innerHTML",et(5,5,t.company),Je),le(2),Ke("innerHTML",et(7,7,t.period),Je),le(3),ln(t.details)}}var $d=class n{content=me(Dt).content;static \u0275fac=function(t){return new(t||n)};static \u0275cmp=Et({type:n,selectors:[["app-experience"]],decls:7,vars:1,consts:[["id","experience","aria-labelledby","experience-title",1,"section"],[1,"container"],["appRevealOnScroll",""],["id","experience-title",3,"title"],[1,"timeline"],["appRevealOnScroll","",1,"surface","timeline-item"],[3,"innerHTML"]],template:function(t,i){t&1&&(Oe(0,"section",0)(1,"div",1)(2,"div",2),st(3,"app-section-title",3),ke(),Oe(4,"div",4),cn(5,HR,11,9,"article",5,BR),ke()()()),t&2&&(le(3),Ke("title",i.content().sectionTitles.experience),le(2),ln(i.content().experiences))},dependencies:[Mn,bn,Rt],styles:[".timeline[_ngcontent-%COMP%]{display:grid;gap:1rem}.timeline-item[_ngcontent-%COMP%]{display:grid;gap:.8rem}.timeline-item[_ngcontent-%COMP%]   header[_ngcontent-%COMP%]{display:grid;gap:.25rem}.timeline-item[_ngcontent-%COMP%]   h3[_ngcontent-%COMP%]{margin:0;font-size:1.05rem}.timeline-item[_ngcontent-%COMP%]   p[_ngcontent-%COMP%]{margin:0;color:var(--text-secondary)}.timeline-item[_ngcontent-%COMP%]   span[_ngcontent-%COMP%]{font-size:.84rem;color:var(--text-tertiary)}.timeline-item[_ngcontent-%COMP%]   ul[_ngcontent-%COMP%]{margin:0;padding-left:1rem;color:var(--text-secondary);display:grid;gap:.35rem}"]})};function zR(n,e){if(n&1){let t=ro();Oe(0,"button",10),so("click",function(){gr(t);let r=Gn();return vr(r.requestEmailAccess())}),bt(1," Afficher l'email "),ke()}if(n&2){let t=Gn();ds("aria-label",t.content().contact.emailLabel)}}function GR(n,e){n&1&&(Oe(0,"p",15),bt(1,"Reponse incorrecte, reessaye."),ke())}function WR(n,e){if(n&1){let t=ro();Oe(0,"div",7)(1,"label",11),bt(2),ke(),Oe(3,"div",12)(4,"input",13),so("input",function(r){gr(t);let s=Gn();return vr(s.updateCaptchaAnswer(r.target.value))}),ke(),Oe(5,"button",14),so("click",function(){gr(t);let r=Gn();return vr(r.validateCaptcha())}),bt(6,"Valider"),ke()(),si(7,GR,2,0,"p",15),ke()}if(n&2){let t=Gn();le(2),Fl("",t.captchaA()," + ",t.captchaB()," = ?"),le(2),Ke("value",t.captchaAnswer()),le(3),oi(t.captchaError()?7:-1)}}function jR(n,e){n&1&&(Oe(0,"a",8),bt(1," philyscor@gmail.com "),ke())}var qd=class n{content=me(Dt).content;captchaVisible=an(!1);emailUnlocked=an(!1);captchaA=an(0);captchaB=an(0);captchaAnswer=an("");captchaError=an(!1);requestEmailAccess(){this.captchaVisible.set(!0),this.emailUnlocked.set(!1),this.captchaAnswer.set(""),this.captchaError.set(!1),this.generateCaptcha()}updateCaptchaAnswer(e){this.captchaAnswer.set(e),this.captchaError.set(!1)}validateCaptcha(){let e=this.captchaA()+this.captchaB();if(Number(this.captchaAnswer())===e){this.emailUnlocked.set(!0),this.captchaVisible.set(!1),this.captchaError.set(!1);return}this.captchaError.set(!0),this.generateCaptcha()}generateCaptcha(){this.captchaA.set(Math.floor(Math.random()*8)+2),this.captchaB.set(Math.floor(Math.random()*8)+2)}static \u0275fac=function(t){return new(t||n)};static \u0275cmp=Et({type:n,selectors:[["app-contact"]],decls:30,vars:19,consts:[["id","contact","aria-labelledby","contact-title",1,"section"],["appRevealOnScroll","",1,"container"],["id","contact-title",3,"title"],[1,"surface","contact-surface"],[3,"innerHTML"],[1,"contact-list"],["type","button",1,"mail-link"],["role","group","aria-label","Captcha email",1,"captcha-box"],["href","mailto:philyscor@gmail.com","aria-label","Email",1,"mail-link"],["target","_blank","rel","noopener noreferrer",3,"href"],["type","button",1,"mail-link",3,"click"],["for","captcha-answer"],[1,"captcha-actions"],["id","captcha-answer","type","number","inputmode","numeric",3,"input","value"],["type","button",1,"captcha-validate",3,"click"],[1,"captcha-error"]],template:function(t,i){t&1&&(Oe(0,"section",0)(1,"div",1),st(2,"app-section-title",2),Oe(3,"div",3),st(4,"h3",4),Qe(5,"richText"),st(6,"p",4),Qe(7,"richText"),Oe(8,"ul",5)(9,"li")(10,"span"),bt(11),ke(),si(12,zR,2,1,"button",6),si(13,WR,8,4,"div",7),si(14,jR,2,0,"a",8),ke(),Oe(15,"li")(16,"span"),bt(17),ke(),Oe(18,"a",9),bt(19),ke()(),Oe(20,"li")(21,"span"),bt(22),ke(),Oe(23,"a",9),bt(24),ke()(),Oe(25,"li")(26,"span"),bt(27),ke(),Oe(28,"strong"),bt(29),ke()()()()()()),t&2&&(le(2),Ke("title",i.content().sectionTitles.contact),le(2),Ke("innerHTML",et(5,15,i.content().contact.title),Je),le(2),Ke("innerHTML",et(7,17,i.content().contact.intro),Je),le(5),Wn(i.content().contact.emailLabel),le(),oi(i.emailUnlocked()?-1:12),le(),oi(i.captchaVisible()?13:-1),le(),oi(i.emailUnlocked()?14:-1),le(3),Wn(i.content().contact.linkedinLabel),le(),Ke("href",i.content().contact.linkedin,qi),le(),Wn(i.content().contact.linkedin),le(3),Wn(i.content().contact.githubLabel),le(),Ke("href",i.content().contact.github,qi),le(),Wn(i.content().contact.github),le(3),Wn(i.content().contact.locationLabel),le(2),Wn(i.content().contact.location))},dependencies:[Mn,bn,Rt],styles:[".contact-surface[_ngcontent-%COMP%]   h3[_ngcontent-%COMP%]{margin:0;font-size:1.15rem}.contact-surface[_ngcontent-%COMP%]   p[_ngcontent-%COMP%]{margin:.8rem 0 1.2rem;color:var(--text-secondary)}.contact-list[_ngcontent-%COMP%]{margin:0;padding:0;list-style:none;display:grid;gap:.65rem}.contact-list[_ngcontent-%COMP%]   li[_ngcontent-%COMP%]{display:grid;gap:.15rem}.contact-list[_ngcontent-%COMP%]   span[_ngcontent-%COMP%]{font-size:.82rem;letter-spacing:.04em;text-transform:uppercase;color:var(--text-tertiary)}.contact-list[_ngcontent-%COMP%]   a[_ngcontent-%COMP%], .contact-list[_ngcontent-%COMP%]   strong[_ngcontent-%COMP%]{color:var(--text-primary);font-weight:500;width:fit-content}.contact-list[_ngcontent-%COMP%]   .mail-link[_ngcontent-%COMP%]{border:none;padding:0;background:transparent;color:var(--text-primary);font:inherit;font-weight:500;cursor:pointer;width:fit-content;text-align:left;text-decoration:underline;text-underline-offset:.18em}.contact-list[_ngcontent-%COMP%]   .captcha-box[_ngcontent-%COMP%]{display:grid;gap:.45rem;margin-top:.35rem}.contact-list[_ngcontent-%COMP%]   .captcha-box[_ngcontent-%COMP%]   label[_ngcontent-%COMP%]{color:var(--text-secondary);font-size:.9rem}.contact-list[_ngcontent-%COMP%]   .captcha-actions[_ngcontent-%COMP%]{display:flex;gap:.5rem;align-items:center}.contact-list[_ngcontent-%COMP%]   .captcha-actions[_ngcontent-%COMP%]   input[_ngcontent-%COMP%]{width:5.2rem;padding:.35rem .45rem;border:1px solid var(--border-color);border-radius:.45rem;background:var(--surface-primary);color:var(--text-primary)}.contact-list[_ngcontent-%COMP%]   .captcha-validate[_ngcontent-%COMP%]{border:1px solid var(--border-color);background:var(--surface-secondary);color:var(--text-primary);border-radius:.45rem;padding:.35rem .55rem;cursor:pointer}.contact-list[_ngcontent-%COMP%]   .captcha-error[_ngcontent-%COMP%]{margin:0;color:#c2410c;font-size:.85rem}"]})};var Xd=class n{content=me(Dt).content;year=new Date().getFullYear();static \u0275fac=function(t){return new(t||n)};static \u0275cmp=Et({type:n,selectors:[["app-footer"]],decls:6,vars:4,consts:[[1,"footer"],[1,"container"],[3,"innerHTML"]],template:function(t,i){t&1&&(kt(0,"footer",0)(1,"div",1)(2,"p"),bt(3),Vt(4,"span",2),Qe(5,"richText"),$t()()()),t&2&&(le(3),pa("",i.year," Portfolio. "),le(),en("innerHTML",et(5,2,i.content().footer.rights),Je))},dependencies:[Rt],styles:[".footer[_ngcontent-%COMP%]{padding:2.2rem 0 3rem;border-top:1px solid var(--border-color)}.footer[_ngcontent-%COMP%]   p[_ngcontent-%COMP%]{margin:0;color:var(--text-tertiary);font-size:.86rem}"]})};var Yd=class n{title=me(Ox);meta=me(Lx);i18n=me(Dt);constructor(){Sh(()=>{let e=this.i18n.language(),t=ao.seo[e];this.title.setTitle(t.title),this.meta.updateTag({name:"description",content:t.description}),this.meta.updateTag({property:"og:title",content:t.title}),this.meta.updateTag({property:"og:description",content:t.description})})}static \u0275fac=function(t){return new(t||n)};static \u0275prov=at({token:n,factory:n.\u0275fac,providedIn:"root"})};var $R=(n,e)=>e.diplome+e.period;function qR(n,e){if(n&1&&(Oe(0,"article",5)(1,"header"),st(2,"h3",6),Qe(3,"richText"),st(4,"p",6),Qe(5,"richText"),st(6,"span",6),Qe(7,"richText"),ke()()),n&2){let t=e.$implicit;le(2),Ke("innerHTML",et(3,3,t.diplome),Je),le(2),Ke("innerHTML",et(5,5,t.school),Je),le(2),Ke("innerHTML",et(7,7,t.period),Je)}}var Zd=class n{content=me(Dt).content;static \u0275fac=function(t){return new(t||n)};static \u0275cmp=Et({type:n,selectors:[["app-school"]],decls:7,vars:1,consts:[["id","school","aria-labelledby","school-title",1,"section"],[1,"container"],["appRevealOnScroll",""],["id","school-title",3,"title"],[1,"timeline"],["appRevealOnScroll","",1,"surface","timeline-item"],[3,"innerHTML"]],template:function(t,i){t&1&&(Oe(0,"section",0)(1,"div",1)(2,"div",2),st(3,"app-section-title",3),ke(),Oe(4,"div",4),cn(5,qR,8,9,"article",5,$R),ke()()()),t&2&&(le(3),Ke("title",i.content().sectionTitles.school),le(2),ln(i.content().school))},dependencies:[Mn,bn,Rt],styles:[".timeline[_ngcontent-%COMP%]{display:grid;gap:1rem}.timeline-item[_ngcontent-%COMP%]{display:grid;gap:.8rem}.timeline-item[_ngcontent-%COMP%]   header[_ngcontent-%COMP%]{display:grid;gap:.25rem}.timeline-item[_ngcontent-%COMP%]   h3[_ngcontent-%COMP%]{margin:0;font-size:1.05rem}.timeline-item[_ngcontent-%COMP%]   p[_ngcontent-%COMP%]{margin:0;color:var(--text-secondary)}.timeline-item[_ngcontent-%COMP%]   span[_ngcontent-%COMP%]{font-size:.84rem;color:var(--text-tertiary)}.timeline-item[_ngcontent-%COMP%]   ul[_ngcontent-%COMP%]{margin:0;padding-left:1rem;color:var(--text-secondary);display:grid;gap:.35rem}"]})};var Jd=class n{seo=me(Yd);constructor(){this.seo}static \u0275fac=function(t){return new(t||n)};static \u0275cmp=Et({type:n,selectors:[["app-root"]],decls:10,vars:0,template:function(t,i){t&1&&(st(0,"app-header"),Oe(1,"main"),st(2,"app-hero")(3,"app-about")(4,"app-projects")(5,"app-skills")(6,"app-experience")(7,"app-school")(8,"app-contact"),ke(),st(9,"app-footer"))},dependencies:[Wl,zd,Gd,Wd,Zd,jd,$d,qd,Xd],styles:["[_nghost-%COMP%]{display:block}main[_ngcontent-%COMP%]{display:block}"]})};om(Jd,Fx).catch(n=>console.error(n));
