var FM=Object.defineProperty,OM=Object.defineProperties;var kM=Object.getOwnPropertyDescriptors;var Wg=Object.getOwnPropertySymbols;var UM=Object.prototype.hasOwnProperty,BM=Object.prototype.propertyIsEnumerable;var jg=(n,e,t)=>e in n?FM(n,e,{enumerable:!0,configurable:!0,writable:!0,value:t}):n[e]=t,It=(n,e)=>{for(var t in e||={})UM.call(e,t)&&jg(n,t,e[t]);if(Wg)for(var t of Wg(e))BM.call(e,t)&&jg(n,t,e[t]);return n},Wt=(n,e)=>OM(n,kM(e));var an=null,vc=!1,yf=1,VM=null,Mn=Symbol("SIGNAL");function Ae(n){let e=an;return an=n,e}function _c(){return an}var Ls={version:0,lastCleanEpoch:0,dirty:!1,producers:void 0,producersTail:void 0,consumers:void 0,consumersTail:void 0,recomputing:!1,consumerAllowSignalWrites:!1,consumerIsAlwaysLive:!1,kind:"unknown",producerMustRecompute:()=>!1,producerRecomputeValue:()=>{},consumerMarkedDirty:()=>{},consumerOnSignalRead:()=>{}};function Uo(n){if(vc)throw new Error("");if(an===null)return;an.consumerOnSignalRead(n);let e=an.producersTail;if(e!==void 0&&e.producer===n)return;let t,i=an.recomputing;if(i&&(t=e!==void 0?e.nextProducer:an.producers,t!==void 0&&t.producer===n)){an.producersTail=t,t.lastReadVersion=n.version;return}let r=n.consumersTail;if(r!==void 0&&r.consumer===an&&(!i||zM(r,an)))return;let s=Os(an),o={producer:n,consumer:an,nextProducer:t,prevConsumer:r,lastReadVersion:n.version,nextConsumer:void 0};an.producersTail=o,e!==void 0?e.nextProducer=o:an.producers=o,s&&Yg(n,o)}function $g(){yf++}function _f(n){if(!(Os(n)&&!n.dirty)&&!(!n.dirty&&n.lastCleanEpoch===yf)){if(!n.producerMustRecompute(n)&&!Vo(n)){vf(n);return}n.producerRecomputeValue(n),vf(n)}}function xf(n){if(n.consumers===void 0)return;let e=vc;vc=!0;try{for(let t=n.consumers;t!==void 0;t=t.nextConsumer){let i=t.consumer;i.dirty||HM(i)}}finally{vc=e}}function Mf(){return an?.consumerAllowSignalWrites!==!1}function HM(n){n.dirty=!0,xf(n),n.consumerMarkedDirty?.(n)}function vf(n){n.dirty=!1,n.lastCleanEpoch=yf}function Fs(n){return n&&qg(n),Ae(n)}function qg(n){n.producersTail=void 0,n.recomputing=!0}function Bo(n,e){Ae(e),n&&Xg(n)}function Xg(n){n.recomputing=!1;let e=n.producersTail,t=e!==void 0?e.nextProducer:n.producers;if(t!==void 0){if(Os(n))do t=bf(t);while(t!==void 0);e!==void 0?e.nextProducer=void 0:n.producers=void 0}}function Vo(n){for(let e=n.producers;e!==void 0;e=e.nextProducer){let t=e.producer,i=e.lastReadVersion;if(i!==t.version||(_f(t),i!==t.version))return!0}return!1}function jr(n){if(Os(n)){let e=n.producers;for(;e!==void 0;)e=bf(e)}n.producers=void 0,n.producersTail=void 0,n.consumers=void 0,n.consumersTail=void 0}function Yg(n,e){let t=n.consumersTail,i=Os(n);if(t!==void 0?(e.nextConsumer=t.nextConsumer,t.nextConsumer=e):(e.nextConsumer=void 0,n.consumers=e),e.prevConsumer=t,n.consumersTail=e,!i)for(let r=n.producers;r!==void 0;r=r.nextProducer)Yg(r.producer,r)}function bf(n){let e=n.producer,t=n.nextProducer,i=n.nextConsumer,r=n.prevConsumer;if(n.nextConsumer=void 0,n.prevConsumer=void 0,i!==void 0?i.prevConsumer=r:e.consumersTail=r,r!==void 0)r.nextConsumer=i;else if(e.consumers=i,!Os(e)){let s=e.producers;for(;s!==void 0;)s=bf(s)}return t}function Os(n){return n.consumerIsAlwaysLive||n.consumers!==void 0}function Ef(n){VM?.(n)}function zM(n,e){let t=e.producersTail;if(t!==void 0){let i=e.producers;do{if(i===n)return!0;if(i===t)break;i=i.nextProducer}while(i!==void 0)}return!1}function Sf(n,e){return Object.is(n,e)}function xc(n,e){let t=Object.create(GM);t.computation=n,e!==void 0&&(t.equal=e);let i=()=>{if(_f(t),Uo(t),t.value===yc)throw t.error;return t.value};return i[Mn]=t,Ef(t),i}var mf=Symbol("UNSET"),gf=Symbol("COMPUTING"),yc=Symbol("ERRORED"),GM=Wt(It({},Ls),{value:mf,dirty:!0,error:null,equal:Sf,kind:"computed",producerMustRecompute(n){return n.value===mf||n.value===gf},producerRecomputeValue(n){if(n.value===gf)throw new Error("");let e=n.value;n.value=gf;let t=Fs(n),i,r=!1;try{i=n.computation(),Ae(null),r=e!==mf&&e!==yc&&i!==yc&&n.equal(e,i)}catch(s){i=yc,n.error=s}finally{Bo(n,t)}if(r){n.value=e;return}n.value=i,n.version++}});function WM(){throw new Error}var Zg=WM;function Jg(n){Zg(n)}function Tf(n){Zg=n}var jM=null;function Cf(n,e){let t=Object.create(Mc);t.value=n,e!==void 0&&(t.equal=e);let i=()=>Kg(t);return i[Mn]=t,Ef(t),[i,o=>Ho(t,o),o=>Qg(t,o)]}function Kg(n){return Uo(n),n.value}function Ho(n,e){Mf()||Jg(n),n.equal(n.value,e)||(n.value=e,$M(n))}function Qg(n,e){Mf()||Jg(n),Ho(n,e(n.value))}var Mc=Wt(It({},Ls),{equal:Sf,value:void 0,kind:"signal"});function $M(n){n.version++,$g(),xf(n),jM?.(n)}var wf=Wt(It({},Ls),{consumerIsAlwaysLive:!0,consumerAllowSignalWrites:!0,dirty:!0,kind:"effect"});function Df(n){if(n.dirty=!1,n.version>0&&!Vo(n))return;n.version++;let e=Fs(n);try{n.cleanup(),n.fn()}finally{Bo(n,e)}}function bn(n){return typeof n=="function"}function bc(n){let t=n(i=>{Error.call(i),i.stack=new Error().stack});return t.prototype=Object.create(Error.prototype),t.prototype.constructor=t,t}var Ec=bc(n=>function(t){n(this),this.message=t?`${t.length} errors occurred during unsubscription:
${t.map((i,r)=>`${r+1}) ${i.toString()}`).join(`
  `)}`:"",this.name="UnsubscriptionError",this.errors=t});function zo(n,e){if(n){let t=n.indexOf(e);0<=t&&n.splice(t,1)}}var yn=class n{constructor(e){this.initialTeardown=e,this.closed=!1,this._parentage=null,this._finalizers=null}unsubscribe(){let e;if(!this.closed){this.closed=!0;let{_parentage:t}=this;if(t)if(this._parentage=null,Array.isArray(t))for(let s of t)s.remove(this);else t.remove(this);let{initialTeardown:i}=this;if(bn(i))try{i()}catch(s){e=s instanceof Ec?s.errors:[s]}let{_finalizers:r}=this;if(r){this._finalizers=null;for(let s of r)try{ev(s)}catch(o){e=e??[],o instanceof Ec?e=[...e,...o.errors]:e.push(o)}}if(e)throw new Ec(e)}}add(e){var t;if(e&&e!==this)if(this.closed)ev(e);else{if(e instanceof n){if(e.closed||e._hasParent(this))return;e._addParent(this)}(this._finalizers=(t=this._finalizers)!==null&&t!==void 0?t:[]).push(e)}}_hasParent(e){let{_parentage:t}=this;return t===e||Array.isArray(t)&&t.includes(e)}_addParent(e){let{_parentage:t}=this;this._parentage=Array.isArray(t)?(t.push(e),t):t?[t,e]:e}_removeParent(e){let{_parentage:t}=this;t===e?this._parentage=null:Array.isArray(t)&&zo(t,e)}remove(e){let{_finalizers:t}=this;t&&zo(t,e),e instanceof n&&e._removeParent(this)}};yn.EMPTY=(()=>{let n=new yn;return n.closed=!0,n})();var If=yn.EMPTY;function Sc(n){return n instanceof yn||n&&"closed"in n&&bn(n.remove)&&bn(n.add)&&bn(n.unsubscribe)}function ev(n){bn(n)?n():n.unsubscribe()}var Qn={onUnhandledError:null,onStoppedNotification:null,Promise:void 0,useDeprecatedSynchronousErrorHandling:!1,useDeprecatedNextContext:!1};var ks={setTimeout(n,e,...t){let{delegate:i}=ks;return i?.setTimeout?i.setTimeout(n,e,...t):setTimeout(n,e,...t)},clearTimeout(n){let{delegate:e}=ks;return(e?.clearTimeout||clearTimeout)(n)},delegate:void 0};function tv(n){ks.setTimeout(()=>{let{onUnhandledError:e}=Qn;if(e)e(n);else throw n})}function Af(){}var nv=Rf("C",void 0,void 0);function iv(n){return Rf("E",void 0,n)}function rv(n){return Rf("N",n,void 0)}function Rf(n,e,t){return{kind:n,value:e,error:t}}var $r=null;function Us(n){if(Qn.useDeprecatedSynchronousErrorHandling){let e=!$r;if(e&&($r={errorThrown:!1,error:null}),n(),e){let{errorThrown:t,error:i}=$r;if($r=null,t)throw i}}else n()}function sv(n){Qn.useDeprecatedSynchronousErrorHandling&&$r&&($r.errorThrown=!0,$r.error=n)}var qr=class extends yn{constructor(e){super(),this.isStopped=!1,e?(this.destination=e,Sc(e)&&e.add(this)):this.destination=YM}static create(e,t,i){return new Bs(e,t,i)}next(e){this.isStopped?Pf(rv(e),this):this._next(e)}error(e){this.isStopped?Pf(iv(e),this):(this.isStopped=!0,this._error(e))}complete(){this.isStopped?Pf(nv,this):(this.isStopped=!0,this._complete())}unsubscribe(){this.closed||(this.isStopped=!0,super.unsubscribe(),this.destination=null)}_next(e){this.destination.next(e)}_error(e){try{this.destination.error(e)}finally{this.unsubscribe()}}_complete(){try{this.destination.complete()}finally{this.unsubscribe()}}},qM=Function.prototype.bind;function Nf(n,e){return qM.call(n,e)}var Lf=class{constructor(e){this.partialObserver=e}next(e){let{partialObserver:t}=this;if(t.next)try{t.next(e)}catch(i){Tc(i)}}error(e){let{partialObserver:t}=this;if(t.error)try{t.error(e)}catch(i){Tc(i)}else Tc(e)}complete(){let{partialObserver:e}=this;if(e.complete)try{e.complete()}catch(t){Tc(t)}}},Bs=class extends qr{constructor(e,t,i){super();let r;if(bn(e)||!e)r={next:e??void 0,error:t??void 0,complete:i??void 0};else{let s;this&&Qn.useDeprecatedNextContext?(s=Object.create(e),s.unsubscribe=()=>this.unsubscribe(),r={next:e.next&&Nf(e.next,s),error:e.error&&Nf(e.error,s),complete:e.complete&&Nf(e.complete,s)}):r=e}this.destination=new Lf(r)}};function Tc(n){Qn.useDeprecatedSynchronousErrorHandling?sv(n):tv(n)}function XM(n){throw n}function Pf(n,e){let{onStoppedNotification:t}=Qn;t&&ks.setTimeout(()=>t(n,e))}var YM={closed:!0,next:Af,error:XM,complete:Af};var ov=typeof Symbol=="function"&&Symbol.observable||"@@observable";function av(n){return n}function cv(n){return n.length===0?av:n.length===1?n[0]:function(t){return n.reduce((i,r)=>r(i),t)}}var Vs=(()=>{class n{constructor(t){t&&(this._subscribe=t)}lift(t){let i=new n;return i.source=this,i.operator=t,i}subscribe(t,i,r){let s=JM(t)?t:new Bs(t,i,r);return Us(()=>{let{operator:o,source:a}=this;s.add(o?o.call(s,a):a?this._subscribe(s):this._trySubscribe(s))}),s}_trySubscribe(t){try{return this._subscribe(t)}catch(i){t.error(i)}}forEach(t,i){return i=lv(i),new i((r,s)=>{let o=new Bs({next:a=>{try{t(a)}catch(c){s(c),o.unsubscribe()}},error:s,complete:r});this.subscribe(o)})}_subscribe(t){var i;return(i=this.source)===null||i===void 0?void 0:i.subscribe(t)}[ov](){return this}pipe(...t){return cv(t)(this)}toPromise(t){return t=lv(t),new t((i,r)=>{let s;this.subscribe(o=>s=o,o=>r(o),()=>i(s))})}}return n.create=e=>new n(e),n})();function lv(n){var e;return(e=n??Qn.Promise)!==null&&e!==void 0?e:Promise}function ZM(n){return n&&bn(n.next)&&bn(n.error)&&bn(n.complete)}function JM(n){return n&&n instanceof qr||ZM(n)&&Sc(n)}function KM(n){return bn(n?.lift)}function uv(n){return e=>{if(KM(e))return e.lift(function(t){try{return n(t,this)}catch(i){this.error(i)}});throw new TypeError("Unable to lift unknown Observable type")}}function dv(n,e,t,i,r){return new Ff(n,e,t,i,r)}var Ff=class extends qr{constructor(e,t,i,r,s,o){super(e),this.onFinalize=s,this.shouldUnsubscribe=o,this._next=t?function(a){try{t(a)}catch(c){e.error(c)}}:super._next,this._error=r?function(a){try{r(a)}catch(c){e.error(c)}finally{this.unsubscribe()}}:super._error,this._complete=i?function(){try{i()}catch(a){e.error(a)}finally{this.unsubscribe()}}:super._complete}unsubscribe(){var e;if(!this.shouldUnsubscribe||this.shouldUnsubscribe()){let{closed:t}=this;super.unsubscribe(),!t&&((e=this.onFinalize)===null||e===void 0||e.call(this))}}};var fv=bc(n=>function(){n(this),this.name="ObjectUnsubscribedError",this.message="object unsubscribed"});var ki=(()=>{class n extends Vs{constructor(){super(),this.closed=!1,this.currentObservers=null,this.observers=[],this.isStopped=!1,this.hasError=!1,this.thrownError=null}lift(t){let i=new Cc(this,this);return i.operator=t,i}_throwIfClosed(){if(this.closed)throw new fv}next(t){Us(()=>{if(this._throwIfClosed(),!this.isStopped){this.currentObservers||(this.currentObservers=Array.from(this.observers));for(let i of this.currentObservers)i.next(t)}})}error(t){Us(()=>{if(this._throwIfClosed(),!this.isStopped){this.hasError=this.isStopped=!0,this.thrownError=t;let{observers:i}=this;for(;i.length;)i.shift().error(t)}})}complete(){Us(()=>{if(this._throwIfClosed(),!this.isStopped){this.isStopped=!0;let{observers:t}=this;for(;t.length;)t.shift().complete()}})}unsubscribe(){this.isStopped=this.closed=!0,this.observers=this.currentObservers=null}get observed(){var t;return((t=this.observers)===null||t===void 0?void 0:t.length)>0}_trySubscribe(t){return this._throwIfClosed(),super._trySubscribe(t)}_subscribe(t){return this._throwIfClosed(),this._checkFinalizedStatuses(t),this._innerSubscribe(t)}_innerSubscribe(t){let{hasError:i,isStopped:r,observers:s}=this;return i||r?If:(this.currentObservers=null,s.push(t),new yn(()=>{this.currentObservers=null,zo(s,t)}))}_checkFinalizedStatuses(t){let{hasError:i,thrownError:r,isStopped:s}=this;i?t.error(r):s&&t.complete()}asObservable(){let t=new Vs;return t.source=this,t}}return n.create=(e,t)=>new Cc(e,t),n})(),Cc=class extends ki{constructor(e,t){super(),this.destination=e,this.source=t}next(e){var t,i;(i=(t=this.destination)===null||t===void 0?void 0:t.next)===null||i===void 0||i.call(t,e)}error(e){var t,i;(i=(t=this.destination)===null||t===void 0?void 0:t.error)===null||i===void 0||i.call(t,e)}complete(){var e,t;(t=(e=this.destination)===null||e===void 0?void 0:e.complete)===null||t===void 0||t.call(e)}_subscribe(e){var t,i;return(i=(t=this.source)===null||t===void 0?void 0:t.subscribe(e))!==null&&i!==void 0?i:If}};var Go=class extends ki{constructor(e){super(),this._value=e}get value(){return this.getValue()}_subscribe(e){let t=super._subscribe(e);return!t.closed&&e.next(this._value),t}getValue(){let{hasError:e,thrownError:t,_value:i}=this;if(e)throw t;return this._throwIfClosed(),i}next(e){super.next(this._value=e)}};function Of(n,e){return uv((t,i)=>{let r=0;t.subscribe(dv(i,s=>{i.next(n.call(e,s,r++))}))})}var kf;function wc(){return kf}function yi(n){let e=kf;return kf=n,e}var hv=Symbol("NotFound");function Hs(n){return n===hv||n?.name==="\u0275NotFound"}var Fc="https://angular.dev/best-practices/security#preventing-cross-site-scripting-xss",He=class extends Error{code;constructor(e,t){super(Yo(e,t)),this.code=e}};function QM(n){return`NG0${Math.abs(n)}`}function Yo(n,e){return`${QM(n)}${e?": "+e:""}`}var hr=globalThis;function vt(n){for(let e in n)if(n[e]===vt)return e;throw Error("")}function Oc(n){if(typeof n=="string")return n;if(Array.isArray(n))return`[${n.map(Oc).join(", ")}]`;if(n==null)return""+n;let e=n.overriddenName||n.name;if(e)return`${e}`;let t=n.toString();if(t==null)return""+t;let i=t.indexOf(`
`);return i>=0?t.slice(0,i):t}function Jf(n,e){return n?e?`${n} ${e}`:n:e||""}var eb=vt({__forward_ref__:vt});function kc(n){return n.__forward_ref__=kc,n}function Cn(n){return yv(n)?n():n}function yv(n){return typeof n=="function"&&n.hasOwnProperty(eb)&&n.__forward_ref__===kc}function at(n){return{token:n.token,providedIn:n.providedIn||null,factory:n.factory,value:void 0}}function Gs(n){return{providers:n.providers||[],imports:n.imports||[]}}function Uc(n){return tb(n,Bc)}function tb(n,e){return n.hasOwnProperty(e)&&n[e]||null}function nb(n){let e=n?.[Bc]??null;return e||null}function Bf(n){return n&&n.hasOwnProperty(Ic)?n[Ic]:null}var Bc=vt({\u0275prov:vt}),Ic=vt({\u0275inj:vt}),Qe=class{_desc;ngMetadataName="InjectionToken";\u0275prov;constructor(e,t){this._desc=e,this.\u0275prov=void 0,typeof t=="number"?this.__NG_ELEMENT_ID__=t:t!==void 0&&(this.\u0275prov=at({token:this,providedIn:t.providedIn||"root",factory:t.factory}))}get multi(){return this}toString(){return`InjectionToken ${this._desc}`}};function Kf(n){return n&&!!n.\u0275providers}var Qf=vt({\u0275cmp:vt}),eh=vt({\u0275dir:vt}),th=vt({\u0275pipe:vt});var Vf=vt({\u0275fac:vt}),Qr=vt({__NG_ELEMENT_ID__:vt}),pv=vt({__NG_ENV_ID__:vt});function es(n){return ih(n,"@Component"),n[Qf]||null}function nh(n){return ih(n,"@Directive"),n[eh]||null}function _v(n){return ih(n,"@Pipe"),n[th]||null}function ih(n,e){if(n==null)throw new He(-919,!1)}function ts(n){return typeof n=="string"?n:n==null?"":String(n)}var xv=vt({ngErrorCode:vt}),ib=vt({ngErrorMessage:vt}),rb=vt({ngTokenPath:vt});function rh(n,e){return Mv("",-200,e)}function Vc(n,e){throw new He(-201,!1)}function Mv(n,e,t){let i=new He(e,n);return i[xv]=e,i[ib]=n,t&&(i[rb]=t),i}function sb(n){return n[xv]}var Hf;function bv(){return Hf}function _n(n){let e=Hf;return Hf=n,e}function sh(n,e,t){let i=Uc(n);if(i&&i.providedIn=="root")return i.value===void 0?i.value=i.factory():i.value;if(t&8)return null;if(e!==void 0)return e;Vc(n,"")}var ob={},Xr=ob,ab="__NG_DI_FLAG__",zf=class{injector;constructor(e){this.injector=e}retrieve(e,t){let i=Yr(t)||0;try{return this.injector.get(e,i&8?null:Xr,i)}catch(r){if(Hs(r))return r;throw r}}};function cb(n,e=0){let t=wc();if(t===void 0)throw new He(-203,!1);if(t===null)return sh(n,void 0,e);{let i=lb(e),r=t.retrieve(n,i);if(Hs(r)){if(i.optional)return null;throw r}return r}}function et(n,e=0){return(bv()||cb)(Cn(n),e)}function pe(n,e){return et(n,Yr(e))}function Yr(n){return typeof n>"u"||typeof n=="number"?n:0|(n.optional&&8)|(n.host&&1)|(n.self&&2)|(n.skipSelf&&4)}function lb(n){return{optional:!!(n&8),host:!!(n&1),self:!!(n&2),skipSelf:!!(n&4)}}function Gf(n){let e=[];for(let t=0;t<n.length;t++){let i=Cn(n[t]);if(Array.isArray(i)){if(i.length===0)throw new He(900,!1);let r,s=0;for(let o=0;o<i.length;o++){let a=i[o],c=ub(a);typeof c=="number"?c===-1?r=a.token:s|=c:r=a}e.push(et(r,s))}else e.push(et(i))}return e}function ub(n){return n[ab]}function Zr(n,e){let t=n.hasOwnProperty(Vf);return t?n[Vf]:null}function Ev(n,e,t){if(n.length!==e.length)return!1;for(let i=0;i<n.length;i++){let r=n[i],s=e[i];if(t&&(r=t(r),s=t(s)),s!==r)return!1}return!0}function Sv(n){return n.flat(Number.POSITIVE_INFINITY)}function Hc(n,e){n.forEach(t=>Array.isArray(t)?Hc(t,e):e(t))}function oh(n,e,t){e>=n.length?n.push(t):n.splice(e,0,t)}function Zo(n,e){return e>=n.length-1?n.pop():n.splice(e,1)[0]}function Tv(n,e,t,i){let r=n.length;if(r==e)n.push(t,i);else if(r===1)n.push(i,n[0]),n[0]=t;else{for(r--,n.push(n[r-1],n[r]);r>e;){let s=r-2;n[r]=n[s],r--}n[e]=t,n[e+1]=i}}function Cv(n,e,t){let i=Ws(n,e);return i>=0?n[i|1]=t:(i=~i,Tv(n,i,e,t)),i}function zc(n,e){let t=Ws(n,e);if(t>=0)return n[t|1]}function Ws(n,e){return db(n,e,1)}function db(n,e,t){let i=0,r=n.length>>t;for(;r!==i;){let s=i+(r-i>>1),o=n[s<<t];if(e===o)return s<<t;o>e?r=s:i=s+1}return~(r<<t)}var ns={},Bn=[],is=new Qe(""),ah=new Qe("",-1),ch=new Qe(""),jo=class{get(e,t=Xr){if(t===Xr){let r=Mv("",-201);throw r.name="\u0275NotFound",r}return t}};function Jo(n){return{\u0275providers:n}}function wv(n){return Jo([{provide:is,multi:!0,useValue:n}])}function Dv(...n){return{\u0275providers:lh(!0,n),\u0275fromNgModule:!0}}function lh(n,...e){let t=[],i=new Set,r,s=o=>{t.push(o)};return Hc(e,o=>{let a=o;Ac(a,s,[],i)&&(r||=[],r.push(a))}),r!==void 0&&Iv(r,s),t}function Iv(n,e){for(let t=0;t<n.length;t++){let{ngModule:i,providers:r}=n[t];uh(r,s=>{e(s,i)})}}function Ac(n,e,t,i){if(n=Cn(n),!n)return!1;let r=null,s=Bf(n),o=!s&&es(n);if(!s&&!o){let c=n.ngModule;if(s=Bf(c),s)r=c;else return!1}else{if(o&&!o.standalone)return!1;r=n}let a=i.has(r);if(o){if(a)return!1;if(i.add(r),o.dependencies){let c=typeof o.dependencies=="function"?o.dependencies():o.dependencies;for(let l of c)Ac(l,e,t,i)}}else if(s){if(s.imports!=null&&!a){i.add(r);let l;Hc(s.imports,u=>{Ac(u,e,t,i)&&(l||=[],l.push(u))}),l!==void 0&&Iv(l,e)}if(!a){let l=Zr(r)||(()=>new r);e({provide:r,useFactory:l,deps:Bn},r),e({provide:ch,useValue:r,multi:!0},r),e({provide:is,useValue:()=>et(r),multi:!0},r)}let c=s.providers;if(c!=null&&!a){let l=n;uh(c,u=>{e(u,l)})}}else return!1;return r!==n&&n.providers!==void 0}function uh(n,e){for(let t of n)Kf(t)&&(t=t.\u0275providers),Array.isArray(t)?uh(t,e):e(t)}var fb=vt({provide:String,useValue:vt});function Av(n){return n!==null&&typeof n=="object"&&fb in n}function hb(n){return!!(n&&n.useExisting)}function pb(n){return!!(n&&n.useFactory)}function Rc(n){return typeof n=="function"}var Ko=new Qe(""),Dc={},mv={},Uf;function Qo(){return Uf===void 0&&(Uf=new jo),Uf}var Vn=class{},Jr=class extends Vn{parent;source;scopes;records=new Map;_ngOnDestroyHooks=new Set;_onDestroyHooks=[];get destroyed(){return this._destroyed}_destroyed=!1;injectorDefTypes;constructor(e,t,i,r){super(),this.parent=t,this.source=i,this.scopes=r,jf(e,o=>this.processProvider(o)),this.records.set(ah,zs(void 0,this)),r.has("environment")&&this.records.set(Vn,zs(void 0,this));let s=this.records.get(Ko);s!=null&&typeof s.value=="string"&&this.scopes.add(s.value),this.injectorDefTypes=new Set(this.get(ch,Bn,{self:!0}))}retrieve(e,t){let i=Yr(t)||0;try{return this.get(e,Xr,i)}catch(r){if(Hs(r))return r;throw r}}destroy(){Wo(this),this._destroyed=!0;let e=Ae(null);try{for(let i of this._ngOnDestroyHooks)i.ngOnDestroy();let t=this._onDestroyHooks;this._onDestroyHooks=[];for(let i of t)i()}finally{this.records.clear(),this._ngOnDestroyHooks.clear(),this.injectorDefTypes.clear(),Ae(e)}}onDestroy(e){return Wo(this),this._onDestroyHooks.push(e),()=>this.removeOnDestroy(e)}runInContext(e){Wo(this);let t=yi(this),i=_n(void 0),r;try{return e()}finally{yi(t),_n(i)}}get(e,t=Xr,i){if(Wo(this),e.hasOwnProperty(pv))return e[pv](this);let r=Yr(i),s,o=yi(this),a=_n(void 0);try{if(!(r&4)){let l=this.records.get(e);if(l===void 0){let u=_b(e)&&Uc(e);u&&this.injectableDefInScope(u)?l=zs(Wf(e),Dc):l=null,this.records.set(e,l)}if(l!=null)return this.hydrate(e,l,r)}let c=r&2?Qo():this.parent;return t=r&8&&t===Xr?null:t,c.get(e,t)}catch(c){let l=sb(c);throw l===-200||l===-201?new He(l,null):c}finally{_n(a),yi(o)}}resolveInjectorInitializers(){let e=Ae(null),t=yi(this),i=_n(void 0),r;try{let s=this.get(is,Bn,{self:!0});for(let o of s)o()}finally{yi(t),_n(i),Ae(e)}}toString(){return"R3Injector[...]"}processProvider(e){e=Cn(e);let t=Rc(e)?e:Cn(e&&e.provide),i=gb(e);if(!Rc(e)&&e.multi===!0){let r=this.records.get(t);r||(r=zs(void 0,Dc,!0),r.factory=()=>Gf(r.multi),this.records.set(t,r)),t=e,r.multi.push(e)}this.records.set(t,i)}hydrate(e,t,i){let r=Ae(null);try{if(t.value===mv)throw rh("");return t.value===Dc&&(t.value=mv,t.value=t.factory(void 0,i)),typeof t.value=="object"&&t.value&&yb(t.value)&&this._ngOnDestroyHooks.add(t.value),t.value}finally{Ae(r)}}injectableDefInScope(e){if(!e.providedIn)return!1;let t=Cn(e.providedIn);return typeof t=="string"?t==="any"||this.scopes.has(t):this.injectorDefTypes.has(t)}removeOnDestroy(e){let t=this._onDestroyHooks.indexOf(e);t!==-1&&this._onDestroyHooks.splice(t,1)}};function Wf(n){let e=Uc(n),t=e!==null?e.factory:Zr(n);if(t!==null)return t;if(n instanceof Qe)throw new He(-204,!1);if(n instanceof Function)return mb(n);throw new He(-204,!1)}function mb(n){if(n.length>0)throw new He(-204,!1);let t=nb(n);return t!==null?()=>t.factory(n):()=>new n}function gb(n){if(Av(n))return zs(void 0,n.useValue);{let e=Rv(n);return zs(e,Dc)}}function Rv(n,e,t){let i;if(Rc(n)){let r=Cn(n);return Zr(r)||Wf(r)}else if(Av(n))i=()=>Cn(n.useValue);else if(pb(n))i=()=>n.useFactory(...Gf(n.deps||[]));else if(hb(n))i=(r,s)=>et(Cn(n.useExisting),s!==void 0&&s&8?8:void 0);else{let r=Cn(n&&(n.useClass||n.provide));if(vb(n))i=()=>new r(...Gf(n.deps));else return Zr(r)||Wf(r)}return i}function Wo(n){if(n.destroyed)throw new He(-205,!1)}function zs(n,e,t=!1){return{factory:n,value:e,multi:t?[]:void 0}}function vb(n){return!!n.deps}function yb(n){return n!==null&&typeof n=="object"&&typeof n.ngOnDestroy=="function"}function _b(n){return typeof n=="function"||typeof n=="object"&&n.ngMetadataName==="InjectionToken"}function jf(n,e){for(let t of n)Array.isArray(t)?jf(t,e):t&&Kf(t)?jf(t.\u0275providers,e):e(t)}function Gc(n,e){let t;n instanceof Jr?(Wo(n),t=n):t=new zf(n);let i,r=yi(t),s=_n(void 0);try{return e()}finally{yi(r),_n(s)}}function Nv(){return bv()!==void 0||wc()!=null}var ti=0,we=1,Oe=2,jt=3,zn=4,Gn=5,js=6,$s=7,kt=8,Hi=9,ni=10,wt=11,qs=12,dh=13,rs=14,Wn=15,pr=16,ss=17,_i=18,zi=19,fh=20,Bi=21,Wc=22,dr=23,wn=24,jc=25,mr=26,Ht=27,Pv=1,hh=6,gr=7,ea=8,os=9,At=10;function Gi(n){return Array.isArray(n)&&typeof n[Pv]=="object"}function ii(n){return Array.isArray(n)&&n[Pv]===!0}function ph(n){return(n.flags&4)!==0}function vr(n){return n.componentOffset>-1}function ta(n){return(n.flags&1)===1}function as(n){return!!n.template}function Xs(n){return(n[Oe]&512)!==0}function cs(n){return(n[Oe]&256)===256}var Lv="svg",Fv="math";function jn(n){for(;Array.isArray(n);)n=n[ti];return n}function mh(n,e){return jn(e[n])}function ri(n,e){return jn(e[n.index])}function $c(n,e){return n.data[e]}function Ov(n,e){return n[e]}function gh(n,e,t,i){t>=n.data.length&&(n.data[t]=null,n.blueprint[t]=null),e[t]=i}function si(n,e){let t=e[n];return Gi(t)?t:t[ti]}function kv(n){return(n[Oe]&4)===4}function qc(n){return(n[Oe]&128)===128}function Uv(n){return ii(n[jt])}function $n(n,e){return e==null?null:n[e]}function vh(n){n[ss]=0}function yh(n){n[Oe]&1024||(n[Oe]|=1024,qc(n)&&Ys(n))}function Bv(n,e){for(;n>0;)e=e[rs],n--;return e}function na(n){return!!(n[Oe]&9216||n[wn]?.dirty)}function Xc(n){n[ni].changeDetectionScheduler?.notify(8),n[Oe]&64&&(n[Oe]|=1024),na(n)&&Ys(n)}function Ys(n){n[ni].changeDetectionScheduler?.notify(0);let e=fr(n);for(;e!==null&&!(e[Oe]&8192||(e[Oe]|=8192,!qc(e)));)e=fr(e)}function _h(n,e){if(cs(n))throw new He(911,!1);n[Bi]===null&&(n[Bi]=[]),n[Bi].push(e)}function Vv(n,e){if(n[Bi]===null)return;let t=n[Bi].indexOf(e);t!==-1&&n[Bi].splice(t,1)}function fr(n){let e=n[jt];return ii(e)?e[jt]:e}function xh(n){return n[$s]??=[]}function Mh(n){return n.cleanup??=[]}function Hv(n,e,t,i){let r=xh(e);r.push(t),n.firstCreatePass&&Mh(n).push(i,r.length-1)}var qe={lFrame:ty(null),bindingsEnabled:!0,skipHydrationRootTNode:null};var $f=!1;function zv(){return qe.lFrame.elementDepthCount}function Gv(){qe.lFrame.elementDepthCount++}function bh(){qe.lFrame.elementDepthCount--}function Eh(){return qe.bindingsEnabled}function Wv(){return qe.skipHydrationRootTNode!==null}function Sh(n){return qe.skipHydrationRootTNode===n}function Th(){qe.skipHydrationRootTNode=null}function Ye(){return qe.lFrame.lView}function nn(){return qe.lFrame.tView}function yr(n){return qe.lFrame.contextLView=n,n[kt]}function _r(n){return qe.lFrame.contextLView=null,n}function Dn(){let n=Ch();for(;n!==null&&n.type===64;)n=n.parent;return n}function Ch(){return qe.lFrame.currentTNode}function jv(){let n=qe.lFrame,e=n.currentTNode;return n.isParent?e:e.parent}function Zs(n,e){let t=qe.lFrame;t.currentTNode=n,t.isParent=e}function wh(){return qe.lFrame.isParent}function $v(){qe.lFrame.isParent=!1}function Dh(){return $f}function $o(n){let e=$f;return $f=n,e}function qv(){let n=qe.lFrame,e=n.bindingRootIndex;return e===-1&&(e=n.bindingRootIndex=n.tView.bindingStartIndex),e}function Xv(){return qe.lFrame.bindingIndex}function Yv(n){return qe.lFrame.bindingIndex=n}function ls(){return qe.lFrame.bindingIndex++}function Ih(n){let e=qe.lFrame,t=e.bindingIndex;return e.bindingIndex=e.bindingIndex+n,t}function Zv(){return qe.lFrame.inI18n}function Jv(n,e){let t=qe.lFrame;t.bindingIndex=t.bindingRootIndex=n,Yc(e)}function Kv(){return qe.lFrame.currentDirectiveIndex}function Yc(n){qe.lFrame.currentDirectiveIndex=n}function Qv(n){let e=qe.lFrame.currentDirectiveIndex;return e===-1?null:n[e]}function Ah(){return qe.lFrame.currentQueryIndex}function Zc(n){qe.lFrame.currentQueryIndex=n}function xb(n){let e=n[we];return e.type===2?e.declTNode:e.type===1?n[Gn]:null}function Rh(n,e,t){if(t&4){let r=e,s=n;for(;r=r.parent,r===null&&!(t&1);)if(r=xb(s),r===null||(s=s[rs],r.type&10))break;if(r===null)return!1;e=r,n=s}let i=qe.lFrame=ey();return i.currentTNode=e,i.lView=n,!0}function Jc(n){let e=ey(),t=n[we];qe.lFrame=e,e.currentTNode=t.firstChild,e.lView=n,e.tView=t,e.contextLView=n,e.bindingIndex=t.bindingStartIndex,e.inI18n=!1}function ey(){let n=qe.lFrame,e=n===null?null:n.child;return e===null?ty(n):e}function ty(n){let e={currentTNode:null,isParent:!0,lView:null,tView:null,selectedIndex:-1,contextLView:null,elementDepthCount:0,currentNamespace:null,currentDirectiveIndex:-1,bindingRootIndex:-1,bindingIndex:-1,currentQueryIndex:0,parent:n,child:null,inI18n:!1};return n!==null&&(n.child=e),e}function ny(){let n=qe.lFrame;return qe.lFrame=n.parent,n.currentTNode=null,n.lView=null,n}var Nh=ny;function Kc(){let n=ny();n.isParent=!0,n.tView=null,n.selectedIndex=-1,n.contextLView=null,n.elementDepthCount=0,n.currentDirectiveIndex=-1,n.currentNamespace=null,n.bindingRootIndex=-1,n.bindingIndex=-1,n.currentQueryIndex=0}function iy(n){return(qe.lFrame.contextLView=Bv(n,qe.lFrame.contextLView))[kt]}function Wi(){return qe.lFrame.selectedIndex}function xr(n){qe.lFrame.selectedIndex=n}function Qc(){let n=qe.lFrame;return $c(n.tView,n.selectedIndex)}function ry(){return qe.lFrame.currentNamespace}var sy=!0;function el(){return sy}function tl(n){sy=n}function qf(n,e=null,t=null,i){let r=oy(n,e,t,i);return r.resolveInjectorInitializers(),r}function oy(n,e=null,t=null,i,r=new Set){let s=[t||Bn,Dv(n)],o;return new Jr(s,e||Qo(),o||null,r)}var ei=class n{static THROW_IF_NOT_FOUND=Xr;static NULL=new jo;static create(e,t){if(Array.isArray(e))return qf({name:""},t,e,"");{let i=e.name??"";return qf({name:i},e.parent,e.providers,i)}}static \u0275prov=at({token:n,providedIn:"any",factory:()=>et(ah)});static __NG_ELEMENT_ID__=-1},cn=new Qe(""),Js=(()=>{class n{static __NG_ELEMENT_ID__=Mb;static __NG_ENV_ID__=t=>t}return n})(),Nc=class extends Js{_lView;constructor(e){super(),this._lView=e}get destroyed(){return cs(this._lView)}onDestroy(e){let t=this._lView;return _h(t,e),()=>Vv(t,e)}};function Mb(){return new Nc(Ye())}var ay=!1,cy=new Qe(""),Ks=(()=>{class n{taskId=0;pendingTasks=new Set;destroyed=!1;pendingTask=new Go(!1);debugTaskTracker=pe(cy,{optional:!0});get hasPendingTasks(){return this.destroyed?!1:this.pendingTask.value}get hasPendingTasksObservable(){return this.destroyed?new Vs(t=>{t.next(!1),t.complete()}):this.pendingTask}add(){!this.hasPendingTasks&&!this.destroyed&&this.pendingTask.next(!0);let t=this.taskId++;return this.pendingTasks.add(t),this.debugTaskTracker?.add(t),t}has(t){return this.pendingTasks.has(t)}remove(t){this.pendingTasks.delete(t),this.debugTaskTracker?.remove(t),this.pendingTasks.size===0&&this.hasPendingTasks&&this.pendingTask.next(!1)}ngOnDestroy(){this.pendingTasks.clear(),this.hasPendingTasks&&this.pendingTask.next(!1),this.destroyed=!0,this.pendingTask.unsubscribe()}static \u0275prov=at({token:n,providedIn:"root",factory:()=>new n})}return n})(),Xf=class extends ki{__isAsync;destroyRef=void 0;pendingTasks=void 0;constructor(e=!1){super(),this.__isAsync=e,Nv()&&(this.destroyRef=pe(Js,{optional:!0})??void 0,this.pendingTasks=pe(Ks,{optional:!0})??void 0)}emit(e){let t=Ae(null);try{super.next(e)}finally{Ae(t)}}subscribe(e,t,i){let r=e,s=t||(()=>null),o=i;if(e&&typeof e=="object"){let c=e;r=c.next?.bind(c),s=c.error?.bind(c),o=c.complete?.bind(c)}this.__isAsync&&(s=this.wrapInTimeout(s),r&&(r=this.wrapInTimeout(r)),o&&(o=this.wrapInTimeout(o)));let a=super.subscribe({next:r,error:s,complete:o});return e instanceof yn&&e.add(a),a}wrapInTimeout(e){return t=>{let i=this.pendingTasks?.add();setTimeout(()=>{try{e(t)}finally{i!==void 0&&this.pendingTasks?.remove(i)}})}}},Ui=Xf;function Pc(...n){}function Ph(n){let e,t;function i(){n=Pc;try{t!==void 0&&typeof cancelAnimationFrame=="function"&&cancelAnimationFrame(t),e!==void 0&&clearTimeout(e)}catch{}}return e=setTimeout(()=>{n(),i()}),typeof requestAnimationFrame=="function"&&(t=requestAnimationFrame(()=>{n(),i()})),()=>i()}function ly(n){return queueMicrotask(()=>n()),()=>{n=Pc}}var Lh="isAngularZone",qo=Lh+"_ID",bb=0,Hn=class n{hasPendingMacrotasks=!1;hasPendingMicrotasks=!1;isStable=!0;onUnstable=new Ui(!1);onMicrotaskEmpty=new Ui(!1);onStable=new Ui(!1);onError=new Ui(!1);constructor(e){let{enableLongStackTrace:t=!1,shouldCoalesceEventChangeDetection:i=!1,shouldCoalesceRunChangeDetection:r=!1,scheduleInRootZone:s=ay}=e;if(typeof Zone>"u")throw new He(908,!1);Zone.assertZonePatched();let o=this;o._nesting=0,o._outer=o._inner=Zone.current,Zone.TaskTrackingZoneSpec&&(o._inner=o._inner.fork(new Zone.TaskTrackingZoneSpec)),t&&Zone.longStackTraceZoneSpec&&(o._inner=o._inner.fork(Zone.longStackTraceZoneSpec)),o.shouldCoalesceEventChangeDetection=!r&&i,o.shouldCoalesceRunChangeDetection=r,o.callbackScheduled=!1,o.scheduleInRootZone=s,Tb(o)}static isInAngularZone(){return typeof Zone<"u"&&Zone.current.get(Lh)===!0}static assertInAngularZone(){if(!n.isInAngularZone())throw new He(909,!1)}static assertNotInAngularZone(){if(n.isInAngularZone())throw new He(909,!1)}run(e,t,i){return this._inner.run(e,t,i)}runTask(e,t,i,r){let s=this._inner,o=s.scheduleEventTask("NgZoneEvent: "+r,e,Eb,Pc,Pc);try{return s.runTask(o,t,i)}finally{s.cancelTask(o)}}runGuarded(e,t,i){return this._inner.runGuarded(e,t,i)}runOutsideAngular(e){return this._outer.run(e)}},Eb={};function Fh(n){if(n._nesting==0&&!n.hasPendingMicrotasks&&!n.isStable)try{n._nesting++,n.onMicrotaskEmpty.emit(null)}finally{if(n._nesting--,!n.hasPendingMicrotasks)try{n.runOutsideAngular(()=>n.onStable.emit(null))}finally{n.isStable=!0}}}function Sb(n){if(n.isCheckStableRunning||n.callbackScheduled)return;n.callbackScheduled=!0;function e(){Ph(()=>{n.callbackScheduled=!1,Yf(n),n.isCheckStableRunning=!0,Fh(n),n.isCheckStableRunning=!1})}n.scheduleInRootZone?Zone.root.run(()=>{e()}):n._outer.run(()=>{e()}),Yf(n)}function Tb(n){let e=()=>{Sb(n)},t=bb++;n._inner=n._inner.fork({name:"angular",properties:{[Lh]:!0,[qo]:t,[qo+t]:!0},onInvokeTask:(i,r,s,o,a,c)=>{if(Cb(c))return i.invokeTask(s,o,a,c);try{return gv(n),i.invokeTask(s,o,a,c)}finally{(n.shouldCoalesceEventChangeDetection&&o.type==="eventTask"||n.shouldCoalesceRunChangeDetection)&&e(),vv(n)}},onInvoke:(i,r,s,o,a,c,l)=>{try{return gv(n),i.invoke(s,o,a,c,l)}finally{n.shouldCoalesceRunChangeDetection&&!n.callbackScheduled&&!wb(c)&&e(),vv(n)}},onHasTask:(i,r,s,o)=>{i.hasTask(s,o),r===s&&(o.change=="microTask"?(n._hasPendingMicrotasks=o.microTask,Yf(n),Fh(n)):o.change=="macroTask"&&(n.hasPendingMacrotasks=o.macroTask))},onHandleError:(i,r,s,o)=>(i.handleError(s,o),n.runOutsideAngular(()=>n.onError.emit(o)),!1)})}function Yf(n){n._hasPendingMicrotasks||(n.shouldCoalesceEventChangeDetection||n.shouldCoalesceRunChangeDetection)&&n.callbackScheduled===!0?n.hasPendingMicrotasks=!0:n.hasPendingMicrotasks=!1}function gv(n){n._nesting++,n.isStable&&(n.isStable=!1,n.onUnstable.emit(null))}function vv(n){n._nesting--,Fh(n)}var Xo=class{hasPendingMicrotasks=!1;hasPendingMacrotasks=!1;isStable=!0;onUnstable=new Ui;onMicrotaskEmpty=new Ui;onStable=new Ui;onError=new Ui;run(e,t,i){return e.apply(t,i)}runGuarded(e,t,i){return e.apply(t,i)}runOutsideAngular(e){return e()}runTask(e,t,i,r){return e.apply(t,i)}};function Cb(n){return uy(n,"__ignore_ng_zone__")}function wb(n){return uy(n,"__scheduler_tick__")}function uy(n,e){return!Array.isArray(n)||n.length!==1?!1:n[0]?.data?.[e]===!0}var Vi=class{_console=console;handleError(e){this._console.error("ERROR",e)}},Mr=new Qe("",{factory:()=>{let n=pe(Hn),e=pe(Vn),t;return i=>{n.runOutsideAngular(()=>{e.destroyed&&!t?setTimeout(()=>{throw i}):(t??=e.get(Vi),t.handleError(i))})}}}),dy={provide:is,useValue:()=>{let n=pe(Vi,{optional:!0})},multi:!0},Db=new Qe("",{factory:()=>{let n=pe(cn).defaultView;if(!n)return;let e=pe(Mr),t=s=>{e(s.reason),s.preventDefault()},i=s=>{s.error?e(s.error):e(new Error(s.message,{cause:s})),s.preventDefault()},r=()=>{n.addEventListener("unhandledrejection",t),n.addEventListener("error",i)};typeof Zone<"u"?Zone.root.run(r):r(),pe(Js).onDestroy(()=>{n.removeEventListener("error",i),n.removeEventListener("unhandledrejection",t)})}});function Oh(){return Jo([wv(()=>{pe(Db)})])}function ln(n,e){let[t,i,r]=Cf(n,e?.equal),s=t,o=s[Mn];return s.set=i,s.update=r,s.asReadonly=fy.bind(s),s}function fy(){let n=this[Mn];if(n.readonlyFn===void 0){let e=()=>this();e[Mn]=n,n.readonlyFn=e}return n.readonlyFn}var nl=(()=>{class n{view;node;constructor(t,i){this.view=t,this.node=i}static __NG_ELEMENT_ID__=Ib}return n})();function Ib(){return new nl(Ye(),Dn())}var Kr=class{},ia=new Qe("",{factory:()=>!0});var kh=new Qe("");var il=(()=>{class n{static \u0275prov=at({token:n,providedIn:"root",factory:()=>new Zf})}return n})(),Zf=class{dirtyEffectCount=0;queues=new Map;add(e){this.enqueue(e),this.schedule(e)}schedule(e){e.dirty&&this.dirtyEffectCount++}remove(e){let t=e.zone,i=this.queues.get(t);i.has(e)&&(i.delete(e),e.dirty&&this.dirtyEffectCount--)}enqueue(e){let t=e.zone;this.queues.has(t)||this.queues.set(t,new Set);let i=this.queues.get(t);i.has(e)||i.add(e)}flush(){for(;this.dirtyEffectCount>0;){let e=!1;for(let[t,i]of this.queues)t===null?e||=this.flushQueue(i):e||=t.run(()=>this.flushQueue(i));e||(this.dirtyEffectCount=0)}}flushQueue(e){let t=!1;for(let i of e)i.dirty&&(this.dirtyEffectCount--,t=!0,i.run());return t}},Lc=class{[Mn];constructor(e){this[Mn]=e}destroy(){this[Mn].destroy()}};function Uh(n,e){let t=e?.injector??pe(ei),i=e?.manualCleanup!==!0?t.get(Js):null,r,s=t.get(nl,null,{optional:!0}),o=t.get(Kr);return s!==null?(r=Nb(s.view,o,n),i instanceof Nc&&i._lView===s.view&&(i=null)):r=Pb(n,t.get(il),o),r.injector=t,i!==null&&(r.onDestroyFns=[i.onDestroy(()=>r.destroy())]),new Lc(r)}var hy=Wt(It({},wf),{cleanupFns:void 0,zone:null,onDestroyFns:null,run(){let n=$o(!1);try{Df(this)}finally{$o(n)}},cleanup(){if(!this.cleanupFns?.length)return;let n=Ae(null);try{for(;this.cleanupFns.length;)this.cleanupFns.pop()()}finally{this.cleanupFns=[],Ae(n)}}}),Ab=Wt(It({},hy),{consumerMarkedDirty(){this.scheduler.schedule(this),this.notifier.notify(12)},destroy(){if(jr(this),this.onDestroyFns!==null)for(let n of this.onDestroyFns)n();this.cleanup(),this.scheduler.remove(this)}}),Rb=Wt(It({},hy),{consumerMarkedDirty(){this.view[Oe]|=8192,Ys(this.view),this.notifier.notify(13)},destroy(){if(jr(this),this.onDestroyFns!==null)for(let n of this.onDestroyFns)n();this.cleanup(),this.view[dr]?.delete(this)}});function Nb(n,e,t){let i=Object.create(Rb);return i.view=n,i.zone=typeof Zone<"u"?Zone.current:null,i.notifier=e,i.fn=py(i,t),n[dr]??=new Set,n[dr].add(i),i.consumerMarkedDirty(i),i}function Pb(n,e,t){let i=Object.create(Ab);return i.fn=py(i,n),i.scheduler=e,i.notifier=t,i.zone=typeof Zone<"u"?Zone.current:null,i.scheduler.add(i),i.notifier.notify(12),i}function py(n,e){return()=>{e(t=>(n.cleanupFns??=[]).push(t))}}function Cl(n){return{toString:n}.toString()}function Wb(n){return typeof n=="function"}function $y(n,e,t,i){e!==null?e.applyValueToInputSignal(e,i):n[t]=i}var hl=class{previousValue;currentValue;firstChange;constructor(e,t,i){this.previousValue=e,this.currentValue=t,this.firstChange=i}isFirstChange(){return this.firstChange}};function jb(n){return n.type.prototype.ngOnChanges&&(n.setInput=qb),$b}function $b(){let n=Xy(this),e=n?.current;if(e){let t=n.previous;if(t===ns)n.previous=e;else for(let i in e)t[i]=e[i];n.current=null,this.ngOnChanges(e)}}function qb(n,e,t,i,r){let s=this.declaredInputs[i],o=Xy(n)||Xb(n,{previous:ns,current:null}),a=o.current||(o.current={}),c=o.previous,l=c[s];a[s]=new hl(l&&l.currentValue,t,c===ns),$y(n,e,r,t)}var qy="__ngSimpleChanges__";function Xy(n){return n[qy]||null}function Xb(n,e){return n[qy]=e}var my=[];var yt=function(n,e=null,t){for(let i=0;i<my.length;i++){let r=my[i];r(n,e,t)}},ct=(function(n){return n[n.TemplateCreateStart=0]="TemplateCreateStart",n[n.TemplateCreateEnd=1]="TemplateCreateEnd",n[n.TemplateUpdateStart=2]="TemplateUpdateStart",n[n.TemplateUpdateEnd=3]="TemplateUpdateEnd",n[n.LifecycleHookStart=4]="LifecycleHookStart",n[n.LifecycleHookEnd=5]="LifecycleHookEnd",n[n.OutputStart=6]="OutputStart",n[n.OutputEnd=7]="OutputEnd",n[n.BootstrapApplicationStart=8]="BootstrapApplicationStart",n[n.BootstrapApplicationEnd=9]="BootstrapApplicationEnd",n[n.BootstrapComponentStart=10]="BootstrapComponentStart",n[n.BootstrapComponentEnd=11]="BootstrapComponentEnd",n[n.ChangeDetectionStart=12]="ChangeDetectionStart",n[n.ChangeDetectionEnd=13]="ChangeDetectionEnd",n[n.ChangeDetectionSyncStart=14]="ChangeDetectionSyncStart",n[n.ChangeDetectionSyncEnd=15]="ChangeDetectionSyncEnd",n[n.AfterRenderHooksStart=16]="AfterRenderHooksStart",n[n.AfterRenderHooksEnd=17]="AfterRenderHooksEnd",n[n.ComponentStart=18]="ComponentStart",n[n.ComponentEnd=19]="ComponentEnd",n[n.DeferBlockStateStart=20]="DeferBlockStateStart",n[n.DeferBlockStateEnd=21]="DeferBlockStateEnd",n[n.DynamicComponentStart=22]="DynamicComponentStart",n[n.DynamicComponentEnd=23]="DynamicComponentEnd",n[n.HostBindingsUpdateStart=24]="HostBindingsUpdateStart",n[n.HostBindingsUpdateEnd=25]="HostBindingsUpdateEnd",n})(ct||{});function Yb(n,e,t){let{ngOnChanges:i,ngOnInit:r,ngDoCheck:s}=e.type.prototype;if(i){let o=jb(e);(t.preOrderHooks??=[]).push(n,o),(t.preOrderCheckHooks??=[]).push(n,o)}r&&(t.preOrderHooks??=[]).push(0-n,r),s&&((t.preOrderHooks??=[]).push(n,s),(t.preOrderCheckHooks??=[]).push(n,s))}function Yy(n,e){for(let t=e.directiveStart,i=e.directiveEnd;t<i;t++){let s=n.data[t].type.prototype,{ngAfterContentInit:o,ngAfterContentChecked:a,ngAfterViewInit:c,ngAfterViewChecked:l,ngOnDestroy:u}=s;o&&(n.contentHooks??=[]).push(-t,o),a&&((n.contentHooks??=[]).push(t,a),(n.contentCheckHooks??=[]).push(t,a)),c&&(n.viewHooks??=[]).push(-t,c),l&&((n.viewHooks??=[]).push(t,l),(n.viewCheckHooks??=[]).push(t,l)),u!=null&&(n.destroyHooks??=[]).push(t,u)}}function cl(n,e,t){Zy(n,e,3,t)}function ll(n,e,t,i){(n[Oe]&3)===t&&Zy(n,e,t,i)}function Bh(n,e){let t=n[Oe];(t&3)===e&&(t&=16383,t+=1,n[Oe]=t)}function Zy(n,e,t,i){let r=i!==void 0?n[ss]&65535:0,s=i??-1,o=e.length-1,a=0;for(let c=r;c<o;c++)if(typeof e[c+1]=="number"){if(a=e[c],i!=null&&a>=i)break}else e[c]<0&&(n[ss]+=65536),(a<s||s==-1)&&(Zb(n,t,e,c),n[ss]=(n[ss]&4294901760)+c+2),c++}function gy(n,e){yt(ct.LifecycleHookStart,n,e);let t=Ae(null);try{e.call(n)}finally{Ae(t),yt(ct.LifecycleHookEnd,n,e)}}function Zb(n,e,t,i){let r=t[i]<0,s=t[i+1],o=r?-t[i]:t[i],a=n[o];r?n[Oe]>>14<n[ss]>>16&&(n[Oe]&3)===e&&(n[Oe]+=16384,gy(a,s)):gy(a,s)}var eo=-1,aa=class{factory;name;injectImpl;resolving=!1;canSeeViewProviders;multi;componentProviders;index;providerFactory;constructor(e,t,i,r){this.factory=e,this.name=r,this.canSeeViewProviders=t,this.injectImpl=i}};function Jb(n){return(n.flags&8)!==0}function Kb(n){return(n.flags&16)!==0}function Qb(n,e,t){let i=0;for(;i<t.length;){let r=t[i];if(typeof r=="number"){if(r!==0)break;i++;let s=t[i++],o=t[i++],a=t[i++];n.setAttribute(e,o,a,s)}else{let s=r,o=t[++i];tE(s)?n.setProperty(e,s,o):n.setAttribute(e,s,o),i++}}return i}function eE(n){return n===3||n===4||n===6}function tE(n){return n.charCodeAt(0)===64}function wl(n,e){if(!(e===null||e.length===0))if(n===null||n.length===0)n=e.slice();else{let t=-1;for(let i=0;i<e.length;i++){let r=e[i];typeof r=="number"?t=r:t===0||(t===-1||t===2?vy(n,t,r,null,e[++i]):vy(n,t,r,null,null))}}return n}function vy(n,e,t,i,r){let s=0,o=n.length;if(e===-1)o=-1;else for(;s<n.length;){let a=n[s++];if(typeof a=="number"){if(a===e){o=-1;break}else if(a>e){o=s-1;break}}}for(;s<n.length;){let a=n[s];if(typeof a=="number")break;if(a===t){r!==null&&(n[s+1]=r);return}s++,r!==null&&s++}o!==-1&&(n.splice(o,0,e),s=o+1),n.splice(s++,0,t),r!==null&&n.splice(s++,0,r)}function Jy(n){return n!==eo}function pl(n){return n&32767}function nE(n){return n>>16}function ml(n,e){let t=nE(n),i=e;for(;t>0;)i=i[rs],t--;return i}var $h=!0;function gl(n){let e=$h;return $h=n,e}var iE=256,Ky=iE-1,Qy=5,rE=0,xi={};function sE(n,e,t){let i;typeof t=="string"?i=t.charCodeAt(0)||0:t.hasOwnProperty(Qr)&&(i=t[Qr]),i==null&&(i=t[Qr]=rE++);let r=i&Ky,s=1<<r;e.data[n+(r>>Qy)]|=s}function e_(n,e){let t=t_(n,e);if(t!==-1)return t;let i=e[we];i.firstCreatePass&&(n.injectorIndex=e.length,Vh(i.data,n),Vh(e,null),Vh(i.blueprint,null));let r=Dp(n,e),s=n.injectorIndex;if(Jy(r)){let o=pl(r),a=ml(r,e),c=a[we].data;for(let l=0;l<8;l++)e[s+l]=a[o+l]|c[o+l]}return e[s+8]=r,s}function Vh(n,e){n.push(0,0,0,0,0,0,0,0,e)}function t_(n,e){return n.injectorIndex===-1||n.parent&&n.parent.injectorIndex===n.injectorIndex||e[n.injectorIndex+8]===null?-1:n.injectorIndex}function Dp(n,e){if(n.parent&&n.parent.injectorIndex!==-1)return n.parent.injectorIndex;let t=0,i=null,r=e;for(;r!==null;){if(i=o_(r),i===null)return eo;if(t++,r=r[rs],i.injectorIndex!==-1)return i.injectorIndex|t<<16}return eo}function oE(n,e,t){sE(n,e,t)}function n_(n,e,t){if(t&8||n!==void 0)return n;Vc(e,"NodeInjector")}function i_(n,e,t,i){if(t&8&&i===void 0&&(i=null),(t&3)===0){let r=n[Hi],s=_n(void 0);try{return r?r.get(e,i,t&8):sh(e,i,t&8)}finally{_n(s)}}return n_(i,e,t)}function r_(n,e,t,i=0,r){if(n!==null){if(e[Oe]&2048&&!(i&2)){let o=uE(n,e,t,i,xi);if(o!==xi)return o}let s=s_(n,e,t,i,xi);if(s!==xi)return s}return i_(e,t,i,r)}function s_(n,e,t,i,r){let s=cE(t);if(typeof s=="function"){if(!Rh(e,n,i))return i&1?n_(r,t,i):i_(e,t,i,r);try{let o;if(o=s(i),o==null&&!(i&8))Vc(t);else return o}finally{Nh()}}else if(typeof s=="number"){let o=null,a=t_(n,e),c=eo,l=i&1?e[Wn][Gn]:null;for((a===-1||i&4)&&(c=a===-1?Dp(n,e):e[a+8],c===eo||!_y(i,!1)?a=-1:(o=e[we],a=pl(c),e=ml(c,e)));a!==-1;){let u=e[we];if(yy(s,a,u.data)){let f=aE(a,e,t,o,i,l);if(f!==xi)return f}c=e[a+8],c!==eo&&_y(i,e[we].data[a+8]===l)&&yy(s,a,e)?(o=u,a=pl(c),e=ml(c,e)):a=-1}}return r}function aE(n,e,t,i,r,s){let o=e[we],a=o.data[n+8],c=i==null?vr(a)&&$h:i!=o&&(a.type&3)!==0,l=r&1&&s===a,u=ul(a,o,t,c,l);return u!==null?vl(e,o,u,a,r):xi}function ul(n,e,t,i,r){let s=n.providerIndexes,o=e.data,a=s&1048575,c=n.directiveStart,l=n.directiveEnd,u=s>>20,f=i?a:a+u,d=r?a+u:l;for(let h=f;h<d;h++){let g=o[h];if(h<c&&t===g||h>=c&&g.type===t)return h}if(r){let h=o[c];if(h&&as(h)&&h.type===t)return c}return null}function vl(n,e,t,i,r){let s=n[t],o=e.data;if(s instanceof aa){let a=s;if(a.resolving)throw rh("");let c=gl(a.canSeeViewProviders);a.resolving=!0;let l=o[t].type||o[t],u,f=a.injectImpl?_n(a.injectImpl):null,d=Rh(n,i,0);try{s=n[t]=a.factory(void 0,r,o,n,i),e.firstCreatePass&&t>=i.directiveStart&&Yb(t,o[t],e)}finally{f!==null&&_n(f),gl(c),a.resolving=!1,Nh()}}return s}function cE(n){if(typeof n=="string")return n.charCodeAt(0)||0;let e=n.hasOwnProperty(Qr)?n[Qr]:void 0;return typeof e=="number"?e>=0?e&Ky:lE:e}function yy(n,e,t){let i=1<<n;return!!(t[e+(n>>Qy)]&i)}function _y(n,e){return!(n&2)&&!(n&1&&e)}var us=class{_tNode;_lView;constructor(e,t){this._tNode=e,this._lView=t}get(e,t,i){return r_(this._tNode,this._lView,e,Yr(i),t)}};function lE(){return new us(Dn(),Ye())}function uE(n,e,t,i,r){let s=n,o=e;for(;s!==null&&o!==null&&o[Oe]&2048&&!Xs(o);){let a=s_(s,o,t,i|2,xi);if(a!==xi)return a;let c=s.parent;if(!c){let l=o[fh];if(l){let u=l.get(t,xi,i&-5);if(u!==xi)return u}c=o_(o),o=o[rs]}s=c}return r}function o_(n){let e=n[we],t=e.type;return t===2?e.declTNode:t===1?n[Gn]:null}function dE(){return ro(Dn(),Ye())}function ro(n,e){return new br(ri(n,e))}var br=(()=>{class n{nativeElement;constructor(t){this.nativeElement=t}static __NG_ELEMENT_ID__=dE}return n})();function fE(n){return n instanceof br?n.nativeElement:n}function hE(){return this._results[Symbol.iterator]()}var yl=class{_emitDistinctChangesOnly;dirty=!0;_onDirty=void 0;_results=[];_changesDetected=!1;_changes=void 0;length=0;first=void 0;last=void 0;get changes(){return this._changes??=new ki}constructor(e=!1){this._emitDistinctChangesOnly=e}get(e){return this._results[e]}map(e){return this._results.map(e)}filter(e){return this._results.filter(e)}find(e){return this._results.find(e)}reduce(e,t){return this._results.reduce(e,t)}forEach(e){this._results.forEach(e)}some(e){return this._results.some(e)}toArray(){return this._results.slice()}toString(){return this._results.toString()}reset(e,t){this.dirty=!1;let i=Sv(e);(this._changesDetected=!Ev(this._results,i,t))&&(this._results=i,this.length=i.length,this.last=i[this.length-1],this.first=i[0])}notifyOnChanges(){this._changes!==void 0&&(this._changesDetected||!this._emitDistinctChangesOnly)&&this._changes.next(this)}onDirty(e){this._onDirty=e}setDirty(){this.dirty=!0,this._onDirty?.()}destroy(){this._changes!==void 0&&(this._changes.complete(),this._changes.unsubscribe())}[Symbol.iterator]=hE};function a_(n){return(n.flags&128)===128}var Ip=(function(n){return n[n.OnPush=0]="OnPush",n[n.Eager=1]="Eager",n[n.Default=1]="Default",n})(Ip||{}),c_=new Map,pE=0;function mE(){return pE++}function gE(n){c_.set(n[zi],n)}function qh(n){c_.delete(n[zi])}var xy="__ngContext__";function to(n,e){Gi(e)?(n[xy]=e[zi],gE(e)):n[xy]=e}function l_(n){return d_(n[qs])}function u_(n){return d_(n[zn])}function d_(n){for(;n!==null&&!ii(n);)n=n[zn];return n}var Xh;function Ap(n){Xh=n}function f_(){if(Xh!==void 0)return Xh;if(typeof document<"u")return document;throw new He(210,!1)}var Dl=new Qe("",{factory:()=>vE}),vE="ng";var Il=new Qe(""),ma=new Qe("",{providedIn:"platform",factory:()=>"unknown"});var Al=new Qe("",{factory:()=>pe(cn).body?.querySelector("[ngCspNonce]")?.getAttribute("ngCspNonce")||null});var h_="r";var p_="di";var m_=!1,g_=new Qe("",{factory:()=>m_});var yE=(n,e,t,i)=>{};function _E(n,e,t,i){yE(n,e,t,i)}function Rp(n){return(n.flags&32)===32}var xE=()=>null;function v_(n,e,t=!1){return xE(n,e,t)}function y_(n,e){let t=n.contentQueries;if(t!==null){let i=Ae(null);try{for(let r=0;r<t.length;r+=2){let s=t[r],o=t[r+1];if(o!==-1){let a=n.data[o];Zc(s),a.contentQueries(2,e[o],o)}}}finally{Ae(i)}}}function Yh(n,e,t){Zc(0);let i=Ae(null);try{e(n,t)}finally{Ae(i)}}function __(n,e,t){if(ph(e)){let i=Ae(null);try{let r=e.directiveStart,s=e.directiveEnd;for(let o=r;o<s;o++){let a=n.data[o];if(a.contentQueries){let c=t[o];a.contentQueries(1,c,o)}}}finally{Ae(i)}}}var ai=(function(n){return n[n.Emulated=0]="Emulated",n[n.None=2]="None",n[n.ShadowDom=3]="ShadowDom",n[n.ExperimentalIsolatedShadowDom=4]="ExperimentalIsolatedShadowDom",n})(ai||{});var rl;function ME(){if(rl===void 0&&(rl=null,hr.trustedTypes))try{rl=hr.trustedTypes.createPolicy("angular",{createHTML:n=>n,createScript:n=>n,createScriptURL:n=>n})}catch{}return rl}function Rl(n){return ME()?.createHTML(n)||n}var sl;function bE(){if(sl===void 0&&(sl=null,hr.trustedTypes))try{sl=hr.trustedTypes.createPolicy("angular#unsafe-bypass",{createHTML:n=>n,createScript:n=>n,createScriptURL:n=>n})}catch{}return sl}function My(n){return bE()?.createHTML(n)||n}var ji=class{changingThisBreaksApplicationSecurity;constructor(e){this.changingThisBreaksApplicationSecurity=e}toString(){return`SafeValue must use [property]=binding: ${this.changingThisBreaksApplicationSecurity} (see ${Fc})`}},Zh=class extends ji{getTypeName(){return"HTML"}},Jh=class extends ji{getTypeName(){return"Style"}},Kh=class extends ji{getTypeName(){return"Script"}},Qh=class extends ji{getTypeName(){return"URL"}},ep=class extends ji{getTypeName(){return"ResourceURL"}};function Ei(n){return n instanceof ji?n.changingThisBreaksApplicationSecurity:n}function $i(n,e){let t=x_(n);if(t!=null&&t!==e){if(t==="ResourceURL"&&e==="URL")return!0;throw new Error(`Required a safe ${e}, got a ${t} (see ${Fc})`)}return t===e}function x_(n){return n instanceof ji&&n.getTypeName()||null}function Np(n){return new Zh(n)}function Pp(n){return new Jh(n)}function Lp(n){return new Kh(n)}function Fp(n){return new Qh(n)}function Op(n){return new ep(n)}function EE(n){let e=new np(n);return SE()?new tp(e):e}var tp=class{inertDocumentHelper;constructor(e){this.inertDocumentHelper=e}getInertBodyElement(e){e="<body><remove></remove>"+e;try{let t=new window.DOMParser().parseFromString(Rl(e),"text/html").body;return t===null?this.inertDocumentHelper.getInertBodyElement(e):(t.firstChild?.remove(),t)}catch{return null}}},np=class{defaultDoc;inertDocument;constructor(e){this.defaultDoc=e,this.inertDocument=this.defaultDoc.implementation.createHTMLDocument("sanitization-inert")}getInertBodyElement(e){let t=this.inertDocument.createElement("template");return t.innerHTML=Rl(e),t}};function SE(){try{return!!new window.DOMParser().parseFromString(Rl(""),"text/html")}catch{return!1}}var TE=/^(?!javascript:)(?:[a-z0-9+.-]+:|[^&:\/?#]*(?:[\/?#]|$))/i;function ga(n){return n=String(n),n.match(TE)?n:"unsafe:"+n}function qi(n){let e={};for(let t of n.split(","))e[t]=!0;return e}function va(...n){let e={};for(let t of n)for(let i in t)t.hasOwnProperty(i)&&(e[i]=!0);return e}var M_=qi("area,br,col,hr,img,wbr"),b_=qi("colgroup,dd,dt,li,p,tbody,td,tfoot,th,thead,tr"),E_=qi("rp,rt"),CE=va(E_,b_),wE=va(b_,qi("address,article,aside,blockquote,caption,center,del,details,dialog,dir,div,dl,figure,figcaption,footer,h1,h2,h3,h4,h5,h6,header,hgroup,hr,ins,main,map,menu,nav,ol,pre,section,summary,table,ul")),DE=va(E_,qi("a,abbr,acronym,audio,b,bdi,bdo,big,br,cite,code,del,dfn,em,font,i,img,ins,kbd,label,map,mark,picture,q,ruby,rp,rt,s,samp,small,source,span,strike,strong,sub,sup,time,track,tt,u,var,video")),by=va(M_,wE,DE,CE),S_=qi("background,cite,href,itemtype,longdesc,poster,src,xlink:href"),IE=qi("abbr,accesskey,align,alt,autoplay,axis,bgcolor,border,cellpadding,cellspacing,class,clear,color,cols,colspan,compact,controls,coords,datetime,default,dir,download,face,headers,height,hidden,hreflang,hspace,ismap,itemscope,itemprop,kind,label,lang,language,loop,media,muted,nohref,nowrap,open,preload,rel,rev,role,rows,rowspan,rules,scope,scrolling,shape,size,sizes,span,srclang,srcset,start,summary,tabindex,target,title,translate,type,usemap,valign,value,vspace,width"),AE=qi("aria-activedescendant,aria-atomic,aria-autocomplete,aria-busy,aria-checked,aria-colcount,aria-colindex,aria-colspan,aria-controls,aria-current,aria-describedby,aria-details,aria-disabled,aria-dropeffect,aria-errormessage,aria-expanded,aria-flowto,aria-grabbed,aria-haspopup,aria-hidden,aria-invalid,aria-keyshortcuts,aria-label,aria-labelledby,aria-level,aria-live,aria-modal,aria-multiline,aria-multiselectable,aria-orientation,aria-owns,aria-placeholder,aria-posinset,aria-pressed,aria-readonly,aria-relevant,aria-required,aria-roledescription,aria-rowcount,aria-rowindex,aria-rowspan,aria-selected,aria-setsize,aria-sort,aria-valuemax,aria-valuemin,aria-valuenow,aria-valuetext"),RE=va(S_,IE,AE),NE=qi("script,style,template");var ip=class{sanitizedSomething=!1;buf=[];sanitizeChildren(e){let t=e.firstChild,i=!0,r=[];for(;t;){if(t.nodeType===Node.ELEMENT_NODE?i=this.startElement(t):t.nodeType===Node.TEXT_NODE?this.chars(t.nodeValue):this.sanitizedSomething=!0,i&&t.firstChild){r.push(t),t=FE(t);continue}for(;t;){t.nodeType===Node.ELEMENT_NODE&&this.endElement(t);let s=LE(t);if(s){t=s;break}t=r.pop()}}return this.buf.join("")}startElement(e){let t=Ey(e).toLowerCase();if(!by.hasOwnProperty(t))return this.sanitizedSomething=!0,!NE.hasOwnProperty(t);this.buf.push("<"),this.buf.push(t);let i=e.attributes;for(let r=0;r<i.length;r++){let s=i.item(r),o=s.name,a=o.toLowerCase();if(!RE.hasOwnProperty(a)){this.sanitizedSomething=!0;continue}let c=s.value;S_[a]&&(c=ga(c)),this.buf.push(" ",o,'="',Sy(c),'"')}return this.buf.push(">"),!0}endElement(e){let t=Ey(e).toLowerCase();by.hasOwnProperty(t)&&!M_.hasOwnProperty(t)&&(this.buf.push("</"),this.buf.push(t),this.buf.push(">"))}chars(e){this.buf.push(Sy(e))}};function PE(n,e){return(n.compareDocumentPosition(e)&Node.DOCUMENT_POSITION_CONTAINED_BY)!==Node.DOCUMENT_POSITION_CONTAINED_BY}function LE(n){let e=n.nextSibling;if(e&&n!==e.previousSibling)throw T_(e);return e}function FE(n){let e=n.firstChild;if(e&&PE(n,e))throw T_(e);return e}function Ey(n){let e=n.nodeName;return typeof e=="string"?e:"FORM"}function T_(n){return new Error(`Failed to sanitize html because the element is clobbered: ${n.outerHTML}`)}var OE=/[\uD800-\uDBFF][\uDC00-\uDFFF]/g,kE=/([^\#-~ |!])/g;function Sy(n){return n.replace(/&/g,"&amp;").replace(OE,function(e){let t=e.charCodeAt(0),i=e.charCodeAt(1);return"&#"+((t-55296)*1024+(i-56320)+65536)+";"}).replace(kE,function(e){return"&#"+e.charCodeAt(0)+";"}).replace(/</g,"&lt;").replace(/>/g,"&gt;")}var ol;function Nl(n,e){let t=null;try{ol=ol||EE(n);let i=e?String(e):"";t=ol.getInertBodyElement(i);let r=5,s=i;do{if(r===0)throw new Error("Failed to sanitize html because the input is unstable");r--,i=s,s=t.innerHTML,t=ol.getInertBodyElement(i)}while(i!==s);let a=new ip().sanitizeChildren(Ty(t)||t);return Rl(a)}finally{if(t){let i=Ty(t)||t;for(;i.firstChild;)i.firstChild.remove()}}}function Ty(n){return"content"in n&&UE(n)?n.content:null}function UE(n){return n.nodeType===Node.ELEMENT_NODE&&n.nodeName==="TEMPLATE"}function BE(n,e){return n.createText(e)}function VE(n,e,t){n.setValue(e,t)}function C_(n,e,t){return n.createElement(e,t)}function _l(n,e,t,i,r){n.insertBefore(e,t,i,r)}function w_(n,e,t){n.appendChild(e,t)}function Cy(n,e,t,i,r){i!==null?_l(n,e,t,i,r):w_(n,e,t)}function D_(n,e,t,i){n.removeChild(null,e,t,i)}function HE(n,e,t){n.setAttribute(e,"style",t)}function zE(n,e,t){t===""?n.removeAttribute(e,"class"):n.setAttribute(e,"class",t)}function I_(n,e,t){let{mergedAttrs:i,classes:r,styles:s}=t;i!==null&&Qb(n,e,i),r!==null&&zE(n,e,r),s!==null&&HE(n,e,s)}var In=(function(n){return n[n.NONE=0]="NONE",n[n.HTML=1]="HTML",n[n.STYLE=2]="STYLE",n[n.SCRIPT=3]="SCRIPT",n[n.URL=4]="URL",n[n.RESOURCE_URL=5]="RESOURCE_URL",n})(In||{});function tt(n){let e=A_();return e?My(e.sanitize(In.HTML,n)||""):$i(n,"HTML")?My(Ei(n)):Nl(f_(),ts(n))}function Xi(n){let e=A_();return e?e.sanitize(In.URL,n)||"":$i(n,"URL")?Ei(n):ga(ts(n))}function A_(){let n=Ye();return n&&n[ni].sanitizer}function GE(n,e,t){let i=n.length;for(;;){let r=n.indexOf(e,t);if(r===-1)return r;if(r===0||n.charCodeAt(r-1)<=32){let s=e.length;if(r+s===i||n.charCodeAt(r+s)<=32)return r}t=r+1}}var R_="ng-template";function WE(n,e,t,i){let r=0;if(i){for(;r<e.length&&typeof e[r]=="string";r+=2)if(e[r]==="class"&&GE(e[r+1].toLowerCase(),t,0)!==-1)return!0}else if(kp(n))return!1;if(r=e.indexOf(1,r),r>-1){let s;for(;++r<e.length&&typeof(s=e[r])=="string";)if(s.toLowerCase()===t)return!0}return!1}function kp(n){return n.type===4&&n.value!==R_}function jE(n,e,t){let i=n.type===4&&!t?R_:n.value;return e===i}function $E(n,e,t){let i=4,r=n.attrs,s=r!==null?YE(r):0,o=!1;for(let a=0;a<e.length;a++){let c=e[a];if(typeof c=="number"){if(!o&&!oi(i)&&!oi(c))return!1;if(o&&oi(c))continue;o=!1,i=c|i&1;continue}if(!o)if(i&4){if(i=2|i&1,c!==""&&!jE(n,c,t)||c===""&&e.length===1){if(oi(i))return!1;o=!0}}else if(i&8){if(r===null||!WE(n,r,c,t)){if(oi(i))return!1;o=!0}}else{let l=e[++a],u=qE(c,r,kp(n),t);if(u===-1){if(oi(i))return!1;o=!0;continue}if(l!==""){let f;if(u>s?f="":f=r[u+1].toLowerCase(),i&2&&l!==f){if(oi(i))return!1;o=!0}}}}return oi(i)||o}function oi(n){return(n&1)===0}function qE(n,e,t,i){if(e===null)return-1;let r=0;if(i||!t){let s=!1;for(;r<e.length;){let o=e[r];if(o===n)return r;if(o===3||o===6)s=!0;else if(o===1||o===2){let a=e[++r];for(;typeof a=="string";)a=e[++r];continue}else{if(o===4)break;if(o===0){r+=4;continue}}r+=s?1:2}return-1}else return ZE(e,n)}function XE(n,e,t=!1){for(let i=0;i<e.length;i++)if($E(n,e[i],t))return!0;return!1}function YE(n){for(let e=0;e<n.length;e++){let t=n[e];if(eE(t))return e}return n.length}function ZE(n,e){let t=n.indexOf(4);if(t>-1)for(t++;t<n.length;){let i=n[t];if(typeof i=="number")return-1;if(i===e)return t;t++}return-1}function wy(n,e){return n?":not("+e.trim()+")":e}function JE(n){let e=n[0],t=1,i=2,r="",s=!1;for(;t<n.length;){let o=n[t];if(typeof o=="string")if(i&2){let a=n[++t];r+="["+o+(a.length>0?'="'+a+'"':"")+"]"}else i&8?r+="."+o:i&4&&(r+=" "+o);else r!==""&&!oi(o)&&(e+=wy(s,r),r=""),i=o,s=s||!oi(i);t++}return r!==""&&(e+=wy(s,r)),e}function KE(n){return n.map(JE).join(",")}function QE(n){let e=[],t=[],i=1,r=2;for(;i<n.length;){let s=n[i];if(typeof s=="string")r===2?s!==""&&e.push(s,n[++i]):r===8&&t.push(s);else{if(!oi(r))break;r=s}i++}return t.length&&e.push(1,...t),e}var qn={};function Up(n,e,t,i,r,s,o,a,c,l,u){let f=Ht+i,d=f+r,h=eS(f,d),g=typeof l=="function"?l():l;return h[we]={type:n,blueprint:h,template:t,queries:null,viewQuery:a,declTNode:e,data:h.slice().fill(null,f),bindingStartIndex:f,expandoStartIndex:d,hostBindingOpCodes:null,firstCreatePass:!0,firstUpdatePass:!0,staticViewQueries:!1,staticContentQueries:!1,preOrderHooks:null,preOrderCheckHooks:null,contentHooks:null,contentCheckHooks:null,viewHooks:null,viewCheckHooks:null,destroyHooks:null,cleanup:null,contentQueries:null,components:null,directiveRegistry:typeof s=="function"?s():s,pipeRegistry:typeof o=="function"?o():o,firstChild:null,schemas:c,consts:g,incompleteFirstPass:!1,ssrId:u}}function eS(n,e){let t=[];for(let i=0;i<e;i++)t.push(i<n?null:qn);return t}function tS(n){let e=n.tView;return e===null||e.incompleteFirstPass?n.tView=Up(1,null,n.template,n.decls,n.vars,n.directiveDefs,n.pipeDefs,n.viewQuery,n.schemas,n.consts,n.id):e}function Bp(n,e,t,i,r,s,o,a,c,l,u){let f=e.blueprint.slice();return f[ti]=r,f[Oe]=i|4|128|8|64|1024,(l!==null||n&&n[Oe]&2048)&&(f[Oe]|=2048),vh(f),f[jt]=f[rs]=n,f[kt]=t,f[ni]=o||n&&n[ni],f[wt]=a||n&&n[wt],f[Hi]=c||n&&n[Hi]||null,f[Gn]=s,f[zi]=mE(),f[js]=u,f[fh]=l,f[Wn]=e.type==2?n[Wn]:f,f}function nS(n,e,t){let i=ri(e,n),r=tS(t),s=n[ni].rendererFactory,o=Vp(n,Bp(n,r,null,N_(t),i,e,null,s.createRenderer(i,t),null,null,null));return n[e.index]=o}function N_(n){let e=16;return n.signals?e=4096:n.onPush&&(e=64),e}function P_(n,e,t,i){if(t===0)return-1;let r=e.length;for(let s=0;s<t;s++)e.push(i),n.blueprint.push(i),n.data.push(null);return r}function Vp(n,e){return n[qs]?n[dh][zn]=e:n[qs]=e,n[dh]=e,e}function oe(n=1){L_(nn(),Ye(),Wi()+n,!1)}function L_(n,e,t,i){if(!i)if((e[Oe]&3)===3){let s=n.preOrderCheckHooks;s!==null&&cl(e,s,t)}else{let s=n.preOrderHooks;s!==null&&ll(e,s,0,t)}xr(t)}var Pl=(function(n){return n[n.None=0]="None",n[n.SignalBased=1]="SignalBased",n[n.HasDecoratorInputTransform=2]="HasDecoratorInputTransform",n})(Pl||{});function rp(n,e,t,i){let r=Ae(null);try{let[s,o,a]=n.inputs[t],c=null;(o&Pl.SignalBased)!==0&&(c=e[s][Mn]),c!==null&&c.transformFn!==void 0?i=c.transformFn(i):a!==null&&(i=a.call(e,i)),n.setInput!==null?n.setInput(e,c,i,t,s):$y(e,c,s,i)}finally{Ae(r)}}var Mi=(function(n){return n[n.Important=1]="Important",n[n.DashCase=2]="DashCase",n})(Mi||{}),iS;function Hp(n,e){return iS(n,e)}var dF=typeof document<"u"&&typeof document?.documentElement?.getAnimations=="function";var sp=new WeakMap,ra=new WeakSet;function rS(n,e){let t=sp.get(n);if(!t||t.length===0)return;let i=e.parentNode,r=e.previousSibling;for(let s=t.length-1;s>=0;s--){let o=t[s],a=o.parentNode;o===e?(t.splice(s,1),ra.add(o),o.dispatchEvent(new CustomEvent("animationend",{detail:{cancel:!0}}))):(r&&o===r||a&&i&&a!==i)&&(t.splice(s,1),o.dispatchEvent(new CustomEvent("animationend",{detail:{cancel:!0}})),o.parentNode?.removeChild(o))}}function sS(n,e){let t=sp.get(n);t?t.includes(e)||t.push(e):sp.set(n,[e])}var ds=new Set,zp=(function(n){return n[n.CHANGE_DETECTION=0]="CHANGE_DETECTION",n[n.AFTER_NEXT_RENDER=1]="AFTER_NEXT_RENDER",n})(zp||{}),so=new Qe(""),Dy=new Set;function oo(n){Dy.has(n)||(Dy.add(n),performance?.mark?.("mark_feature_usage",{detail:{feature:n}}))}var F_=(()=>{class n{impl=null;execute(){this.impl?.execute()}static \u0275prov=at({token:n,providedIn:"root",factory:()=>new n})}return n})();var O_=new Qe("",{factory:()=>({queue:new Set,isScheduled:!1,scheduler:null,injector:pe(Vn)})});function k_(n,e,t){let i=n.get(O_);if(Array.isArray(e))for(let r of e)i.queue.add(r),t?.detachedLeaveAnimationFns?.push(r);else i.queue.add(e),t?.detachedLeaveAnimationFns?.push(e);i.scheduler&&i.scheduler(n)}function oS(n,e){let t=n.get(O_);if(e.detachedLeaveAnimationFns){for(let i of e.detachedLeaveAnimationFns)t.queue.delete(i);e.detachedLeaveAnimationFns=void 0}}function aS(n,e){for(let[t,i]of e)k_(n,i.animateFns)}function Iy(n,e,t,i){let r=n?.[mr]?.enter;e!==null&&r&&r.has(t.index)&&aS(i,r)}function Qs(n,e,t,i,r,s,o,a){if(r!=null){let c,l=!1;ii(r)?c=r:Gi(r)&&(l=!0,r=r[ti]);let u=jn(r);n===0&&i!==null?(Iy(a,i,s,t),o==null?w_(e,i,u):_l(e,i,u,o||null,!0)):n===1&&i!==null?(Iy(a,i,s,t),_l(e,i,u,o||null,!0),rS(s,u)):n===2?(a?.[mr]?.leave?.has(s.index)&&sS(s,u),ra.delete(u),Ay(a,s,t,f=>{if(ra.has(u)){ra.delete(u);return}D_(e,u,l,f)})):n===3&&(ra.delete(u),Ay(a,s,t,()=>{e.destroyNode(u)})),c!=null&&xS(e,n,t,c,s,i,o)}}function cS(n,e){U_(n,e),e[ti]=null,e[Gn]=null}function lS(n,e,t,i,r,s){i[ti]=r,i[Gn]=e,Fl(n,i,t,1,r,s)}function U_(n,e){e[ni].changeDetectionScheduler?.notify(9),Fl(n,e,e[wt],2,null,null)}function uS(n){let e=n[qs];if(!e)return Hh(n[we],n);for(;e;){let t=null;if(Gi(e))t=e[qs];else{let i=e[At];i&&(t=i)}if(!t){for(;e&&!e[zn]&&e!==n;)Gi(e)&&Hh(e[we],e),e=e[jt];e===null&&(e=n),Gi(e)&&Hh(e[we],e),t=e&&e[zn]}e=t}}function Gp(n,e){let t=n[os],i=t.indexOf(e);t.splice(i,1)}function Ll(n,e){if(cs(e))return;let t=e[wt];t.destroyNode&&Fl(n,e,t,3,null,null),uS(e)}function Hh(n,e){if(cs(e))return;let t=Ae(null);try{e[Oe]&=-129,e[Oe]|=256,e[wn]&&jr(e[wn]),hS(n,e),fS(n,e),e[we].type===1&&e[wt].destroy();let i=e[pr];if(i!==null&&ii(e[jt])){i!==e[jt]&&Gp(i,e);let r=e[_i];r!==null&&r.detachView(n)}qh(e)}finally{Ae(t)}}function Ay(n,e,t,i){let r=n?.[mr];if(r==null||r.leave==null||!r.leave.has(e.index))return i(!1);n&&ds.add(n[zi]),k_(t,()=>{if(r.leave&&r.leave.has(e.index)){let o=r.leave.get(e.index),a=[];if(o){for(let c=0;c<o.animateFns.length;c++){let l=o.animateFns[c],{promise:u}=l();a.push(u)}r.detachedLeaveAnimationFns=void 0}r.running=Promise.allSettled(a),dS(n,i)}else n&&ds.delete(n[zi]),i(!1)},r)}function dS(n,e){let t=n[mr]?.running;if(t){t.then(()=>{n[mr].running=void 0,ds.delete(n[zi]),e(!0)});return}e(!1)}function fS(n,e){let t=n.cleanup,i=e[$s];if(t!==null)for(let o=0;o<t.length-1;o+=2)if(typeof t[o]=="string"){let a=t[o+3];a>=0?i[a]():i[-a].unsubscribe(),o+=2}else{let a=i[t[o+1]];t[o].call(a)}i!==null&&(e[$s]=null);let r=e[Bi];if(r!==null){e[Bi]=null;for(let o=0;o<r.length;o++){let a=r[o];a()}}let s=e[dr];if(s!==null){e[dr]=null;for(let o of s)o.destroy()}}function hS(n,e){let t;if(n!=null&&(t=n.destroyHooks)!=null)for(let i=0;i<t.length;i+=2){let r=e[t[i]];if(!(r instanceof aa)){let s=t[i+1];if(Array.isArray(s))for(let o=0;o<s.length;o+=2){let a=r[s[o]],c=s[o+1];yt(ct.LifecycleHookStart,a,c);try{c.call(a)}finally{yt(ct.LifecycleHookEnd,a,c)}}else{yt(ct.LifecycleHookStart,r,s);try{s.call(r)}finally{yt(ct.LifecycleHookEnd,r,s)}}}}}function pS(n,e,t){return mS(n,e.parent,t)}function mS(n,e,t){let i=e;for(;i!==null&&i.type&168;)e=i,i=e.parent;if(i===null)return t[ti];if(vr(i)){let{encapsulation:r}=n.data[i.directiveStart+i.componentOffset];if(r===ai.None||r===ai.Emulated)return null}return ri(i,t)}function gS(n,e,t){return yS(n,e,t)}function vS(n,e,t){return n.type&40?ri(n,t):null}var yS=vS,Ry;function Wp(n,e,t,i){let r=pS(n,i,e),s=e[wt],o=i.parent||e[Gn],a=gS(o,i,e);if(r!=null)if(Array.isArray(t))for(let c=0;c<t.length;c++)Cy(s,r,t[c],a,!1);else Cy(s,r,t,a,!1);Ry!==void 0&&Ry(s,i,e,t,r)}function sa(n,e){if(e!==null){let t=e.type;if(t&3)return ri(e,n);if(t&4)return op(-1,n[e.index]);if(t&8){let i=e.child;if(i!==null)return sa(n,i);{let r=n[e.index];return ii(r)?op(-1,r):jn(r)}}else{if(t&128)return sa(n,e.next);if(t&32)return Hp(e,n)()||jn(n[e.index]);{let i=B_(n,e);if(i!==null){if(Array.isArray(i))return i[0];let r=fr(n[Wn]);return sa(r,i)}else return sa(n,e.next)}}}return null}function B_(n,e){if(e!==null){let i=n[Wn][Gn],r=e.projection;return i.projection[r]}return null}function op(n,e){let t=At+n+1;if(t<e.length){let i=e[t],r=i[we].firstChild;if(r!==null)return sa(i,r)}return e[gr]}function jp(n,e,t,i,r,s,o){for(;t!=null;){let a=i[Hi];if(t.type===128){t=t.next;continue}let c=i[t.index],l=t.type;if(o&&e===0&&(c&&to(jn(c),i),t.flags|=2),!Rp(t))if(l&8)jp(n,e,t.child,i,r,s,!1),Qs(e,n,a,r,c,t,s,i);else if(l&32){let u=Hp(t,i),f;for(;f=u();)Qs(e,n,a,r,f,t,s,i);Qs(e,n,a,r,c,t,s,i)}else l&16?_S(n,e,i,t,r,s):Qs(e,n,a,r,c,t,s,i);t=o?t.projectionNext:t.next}}function Fl(n,e,t,i,r,s){jp(t,i,n.firstChild,e,r,s,!1)}function _S(n,e,t,i,r,s){let o=t[Wn],c=o[Gn].projection[i.projection];if(Array.isArray(c))for(let l=0;l<c.length;l++){let u=c[l];Qs(e,n,t[Hi],r,u,i,s,t)}else{let l=c,u=o[jt];a_(i)&&(l.flags|=128),jp(n,e,l,u,r,s,!0)}}function xS(n,e,t,i,r,s,o){let a=i[gr],c=jn(i);a!==c&&Qs(e,n,t,s,a,r,o);for(let l=At;l<i.length;l++){let u=i[l];Fl(u[we],u,n,e,s,a)}}function MS(n,e,t,i,r){if(e)r?n.addClass(t,i):n.removeClass(t,i);else{let s=i.indexOf("-")===-1?void 0:Mi.DashCase;r==null?n.removeStyle(t,i,s):(typeof r=="string"&&r.endsWith("!important")&&(r=r.slice(0,-10),s|=Mi.Important),n.setStyle(t,i,r,s))}}function V_(n,e,t,i,r){let s=Wi(),o=i&2;try{xr(-1),o&&e.length>Ht&&L_(n,e,Ht,!1);let a=o?ct.TemplateUpdateStart:ct.TemplateCreateStart;yt(a,r,t),t(i,r)}finally{xr(s);let a=o?ct.TemplateUpdateEnd:ct.TemplateCreateEnd;yt(a,r,t)}}function $p(n,e,t){DS(n,e,t),(t.flags&64)===64&&IS(n,e,t)}function Ol(n,e,t=ri){let i=e.localNames;if(i!==null){let r=e.index+1;for(let s=0;s<i.length;s+=2){let o=i[s+1],a=o===-1?t(e,n):n[o];n[r++]=a}}}function bS(n,e,t,i){let s=i.get(g_,m_)||t===ai.ShadowDom||t===ai.ExperimentalIsolatedShadowDom,o=n.selectRootElement(e,s);return ES(o),o}function ES(n){SS(n)}var SS=()=>null;function TS(n){return n==="class"?"className":n==="for"?"htmlFor":n==="formaction"?"formAction":n==="innerHtml"?"innerHTML":n==="readonly"?"readOnly":n==="tabindex"?"tabIndex":n}function CS(n,e,t,i,r,s){let o=e[we];if(qp(n,o,e,t,i)){vr(n)&&wS(e,n.index);return}n.type&3&&(t=TS(t)),H_(n,e,t,i,r,s)}function H_(n,e,t,i,r,s){if(n.type&3){let o=ri(n,e);i=s!=null?s(i,n.value||"",t):i,r.setProperty(o,t,i)}else n.type&12}function wS(n,e){let t=si(e,n);t[Oe]&16||(t[Oe]|=64)}function DS(n,e,t){let i=t.directiveStart,r=t.directiveEnd;vr(t)&&nS(e,t,n.data[i+t.componentOffset]),n.firstCreatePass||e_(t,e);let s=t.initialInputs;for(let o=i;o<r;o++){let a=n.data[o],c=vl(e,n,o,t);if(to(c,e),s!==null&&PS(e,o-i,c,a,t,s),as(a)){let l=si(t.index,e);l[kt]=vl(e,n,o,t)}}}function IS(n,e,t){let i=t.directiveStart,r=t.directiveEnd,s=t.index,o=Kv();try{xr(s);for(let a=i;a<r;a++){let c=n.data[a],l=e[a];Yc(a),(c.hostBindings!==null||c.hostVars!==0||c.hostAttrs!==null)&&AS(c,l)}}finally{xr(-1),Yc(o)}}function AS(n,e){n.hostBindings!==null&&n.hostBindings(1,e)}function z_(n,e){let t=n.directiveRegistry,i=null;if(t)for(let r=0;r<t.length;r++){let s=t[r];XE(e,s.selectors,!1)&&(i??=[],as(s)?i.unshift(s):i.push(s))}return i}function RS(n,e,t,i,r,s){let o=ri(n,e);NS(e[wt],o,s,n.value,t,i,r)}function NS(n,e,t,i,r,s,o){if(s==null)n.removeAttribute(e,r,t);else{let a=o==null?ts(s):o(s,i||"",r);n.setAttribute(e,r,a,t)}}function PS(n,e,t,i,r,s){let o=s[e];if(o!==null)for(let a=0;a<o.length;a+=2){let c=o[a],l=o[a+1];rp(i,t,c,l)}}function G_(n,e,t,i,r){let s=Ht+t,o=e[we],a=r(o,e,n,i,t);e[s]=a,Zs(n,!0);let c=n.type===2;return c?(I_(e[wt],a,n),(zv()===0||ta(n))&&to(a,e),Gv()):to(a,e),el()&&(!c||!Rp(n))&&Wp(o,e,a,n),n}function W_(n){let e=n;return wh()?$v():(e=e.parent,Zs(e,!1)),e}function LS(n,e){let t=n[Hi];if(!t)return;let i;try{i=t.get(Mr,null)}catch{i=null}i?.(e)}function qp(n,e,t,i,r){let s=n.inputs?.[i],o=n.hostDirectiveInputs?.[i],a=!1;if(o)for(let c=0;c<o.length;c+=2){let l=o[c],u=o[c+1],f=e.data[l];rp(f,t[l],u,r),a=!0}if(s)for(let c of s){let l=t[c],u=e.data[c];rp(u,l,i,r),a=!0}return a}function FS(n,e){let t=si(e,n),i=t[we];OS(i,t);let r=t[ti];r!==null&&t[js]===null&&(t[js]=v_(r,t[Hi])),yt(ct.ComponentStart);try{Xp(i,t,t[kt])}finally{yt(ct.ComponentEnd,t[kt])}}function OS(n,e){for(let t=e.length;t<n.blueprint.length;t++)e.push(n.blueprint[t])}function Xp(n,e,t){Jc(e);try{let i=n.viewQuery;i!==null&&Yh(1,i,t);let r=n.template;r!==null&&V_(n,e,r,1,t),n.firstCreatePass&&(n.firstCreatePass=!1),e[_i]?.finishViewCreation(n),n.staticContentQueries&&y_(n,e),n.staticViewQueries&&Yh(2,n.viewQuery,t);let s=n.components;s!==null&&kS(e,s)}catch(i){throw n.firstCreatePass&&(n.incompleteFirstPass=!0,n.firstCreatePass=!1),i}finally{e[Oe]&=-5,Kc()}}function kS(n,e){for(let t=0;t<e.length;t++)FS(n,e[t])}function kl(n,e,t,i){let r=Ae(null);try{let s=e.tView,a=n[Oe]&4096?4096:16,c=Bp(n,s,t,a,null,e,null,null,i?.injector??null,i?.embeddedViewInjector??null,i?.dehydratedView??null),l=n[e.index];c[pr]=l;let u=n[_i];return u!==null&&(c[_i]=u.createEmbeddedView(s)),Xp(s,c,t),c}finally{Ae(r)}}function ca(n,e){return!e||e.firstChild===null||a_(n)}function la(n,e,t,i,r=!1){for(;t!==null;){if(t.type===128){t=r?t.projectionNext:t.next;continue}let s=e[t.index];s!==null&&i.push(jn(s)),ii(s)&&j_(s,i);let o=t.type;if(o&8)la(n,e,t.child,i);else if(o&32){let a=Hp(t,e),c;for(;c=a();)i.push(c)}else if(o&16){let a=B_(e,t);if(Array.isArray(a))i.push(...a);else{let c=fr(e[Wn]);la(c[we],c,a,i,!0)}}t=r?t.projectionNext:t.next}return i}function j_(n,e){for(let t=At;t<n.length;t++){let i=n[t],r=i[we].firstChild;r!==null&&la(i[we],i,r,e)}n[gr]!==n[ti]&&e.push(n[gr])}function $_(n){if(n[jc]!==null){for(let e of n[jc])e.impl.addSequence(e);n[jc].length=0}}var q_=[];function US(n){return n[wn]??BS(n)}function BS(n){let e=q_.pop()??Object.create(HS);return e.lView=n,e}function VS(n){n.lView[wn]!==n&&(n.lView=null,q_.push(n))}var HS=Wt(It({},Ls),{consumerIsAlwaysLive:!0,kind:"template",consumerMarkedDirty:n=>{Ys(n.lView)},consumerOnSignalRead(){this.lView[wn]=this}});function zS(n){let e=n[wn]??Object.create(GS);return e.lView=n,e}var GS=Wt(It({},Ls),{consumerIsAlwaysLive:!0,kind:"template",consumerMarkedDirty:n=>{let e=fr(n.lView);for(;e&&!X_(e[we]);)e=fr(e);e&&yh(e)},consumerOnSignalRead(){this.lView[wn]=this}});function X_(n){return n.type!==2}function Y_(n){if(n[dr]===null)return;let e=!0;for(;e;){let t=!1;for(let i of n[dr])i.dirty&&(t=!0,i.zone===null||Zone.current===i.zone?i.run():i.zone.run(()=>i.run()));e=t&&!!(n[Oe]&8192)}}var WS=100;function Z_(n,e=0){let i=n[ni].rendererFactory,r=!1;r||i.begin?.();try{jS(n,e)}finally{r||i.end?.()}}function jS(n,e){let t=Dh();try{$o(!0),ap(n,e);let i=0;for(;na(n);){if(i===WS)throw new He(103,!1);i++,ap(n,1)}}finally{$o(t)}}function $S(n,e,t,i){if(cs(e))return;let r=e[Oe],s=!1,o=!1;Jc(e);let a=!0,c=null,l=null;s||(X_(n)?(l=US(e),c=Fs(l)):_c()===null?(a=!1,l=zS(e),c=Fs(l)):e[wn]&&(jr(e[wn]),e[wn]=null));try{vh(e),Yv(n.bindingStartIndex),t!==null&&V_(n,e,t,2,i);let u=(r&3)===3;if(!s)if(u){let h=n.preOrderCheckHooks;h!==null&&cl(e,h,null)}else{let h=n.preOrderHooks;h!==null&&ll(e,h,0,null),Bh(e,0)}if(o||qS(e),Y_(e),J_(e,0),n.contentQueries!==null&&y_(n,e),!s)if(u){let h=n.contentCheckHooks;h!==null&&cl(e,h)}else{let h=n.contentHooks;h!==null&&ll(e,h,1),Bh(e,1)}YS(n,e);let f=n.components;f!==null&&Q_(e,f,0);let d=n.viewQuery;if(d!==null&&Yh(2,d,i),!s)if(u){let h=n.viewCheckHooks;h!==null&&cl(e,h)}else{let h=n.viewHooks;h!==null&&ll(e,h,2),Bh(e,2)}if(n.firstUpdatePass===!0&&(n.firstUpdatePass=!1),e[Wc]){for(let h of e[Wc])h();e[Wc]=null}s||($_(e),e[Oe]&=-73)}catch(u){throw s||Ys(e),u}finally{l!==null&&(Bo(l,c),a&&VS(l)),Kc()}}function J_(n,e){for(let t=l_(n);t!==null;t=u_(t))for(let i=At;i<t.length;i++){let r=t[i];K_(r,e)}}function qS(n){for(let e=l_(n);e!==null;e=u_(e)){if(!(e[Oe]&2))continue;let t=e[os];for(let i=0;i<t.length;i++){let r=t[i];yh(r)}}}function XS(n,e,t){yt(ct.ComponentStart);let i=si(e,n);try{K_(i,t)}finally{yt(ct.ComponentEnd,i[kt])}}function K_(n,e){qc(n)&&ap(n,e)}function ap(n,e){let i=n[we],r=n[Oe],s=n[wn],o=!!(e===0&&r&16);if(o||=!!(r&64&&e===0),o||=!!(r&1024),o||=!!(s?.dirty&&Vo(s)),o||=!1,s&&(s.dirty=!1),n[Oe]&=-9217,o)$S(i,n,i.template,n[kt]);else if(r&8192){let a=Ae(null);try{Y_(n),J_(n,1);let c=i.components;c!==null&&Q_(n,c,1),$_(n)}finally{Ae(a)}}}function Q_(n,e,t){for(let i=0;i<e.length;i++)XS(n,e[i],t)}function YS(n,e){let t=n.hostBindingOpCodes;if(t!==null)try{for(let i=0;i<t.length;i++){let r=t[i];if(r<0)xr(~r);else{let s=r,o=t[++i],a=t[++i];Jv(o,s);let c=e[s];yt(ct.HostBindingsUpdateStart,c);try{a(2,c)}finally{yt(ct.HostBindingsUpdateEnd,c)}}}}finally{xr(-1)}}function Yp(n,e){let t=Dh()?64:1088;for(n[ni].changeDetectionScheduler?.notify(e);n;){n[Oe]|=t;let i=fr(n);if(Xs(n)&&!i)return n;n=i}return null}function e0(n,e,t,i){return[n,!0,0,e,null,i,null,t,null,null]}function t0(n,e){let t=At+e;if(t<n.length)return n[t]}function Ul(n,e,t,i=!0){let r=e[we];if(ZS(r,e,n,t),i){let o=op(t,n),a=e[wt],c=a.parentNode(n[gr]);c!==null&&lS(r,n[Gn],a,e,c,o)}let s=e[js];s!==null&&s.firstChild!==null&&(s.firstChild=null)}function n0(n,e){let t=ua(n,e);return t!==void 0&&Ll(t[we],t),t}function ua(n,e){if(n.length<=At)return;let t=At+e,i=n[t];if(i){let r=i[pr];r!==null&&r!==n&&Gp(r,i),e>0&&(n[t-1][zn]=i[zn]);let s=Zo(n,At+e);cS(i[we],i);let o=s[_i];o!==null&&o.detachView(s[we]),i[jt]=null,i[zn]=null,i[Oe]&=-129}return i}function ZS(n,e,t,i){let r=At+i,s=t.length;i>0&&(t[r-1][zn]=e),i<s-At?(e[zn]=t[r],oh(t,At+i,e)):(t.push(e),e[zn]=null),e[jt]=t;let o=e[pr];o!==null&&t!==o&&i0(o,e);let a=e[_i];a!==null&&a.insertView(n),Xc(e),e[Oe]|=128}function i0(n,e){let t=n[os],i=e[jt];if(Gi(i))n[Oe]|=2;else{let r=i[jt][Wn];e[Wn]!==r&&(n[Oe]|=2)}t===null?n[os]=[e]:t.push(e)}var no=class{_lView;_cdRefInjectingView;_appRef=null;_attachedToViewContainer=!1;exhaustive;get rootNodes(){let e=this._lView,t=e[we];return la(t,e,t.firstChild,[])}constructor(e,t){this._lView=e,this._cdRefInjectingView=t}get context(){return this._lView[kt]}set context(e){this._lView[kt]=e}get destroyed(){return cs(this._lView)}destroy(){if(this._appRef)this._appRef.detachView(this);else if(this._attachedToViewContainer){let e=this._lView[jt];if(ii(e)){let t=e[ea],i=t?t.indexOf(this):-1;i>-1&&(ua(e,i),Zo(t,i))}this._attachedToViewContainer=!1}Ll(this._lView[we],this._lView)}onDestroy(e){_h(this._lView,e)}markForCheck(){Yp(this._cdRefInjectingView||this._lView,4)}detach(){this._lView[Oe]&=-129}reattach(){Xc(this._lView),this._lView[Oe]|=128}detectChanges(){this._lView[Oe]|=1024,Z_(this._lView)}checkNoChanges(){}attachToViewContainerRef(){if(this._appRef)throw new He(902,!1);this._attachedToViewContainer=!0}detachFromAppRef(){this._appRef=null;let e=Xs(this._lView),t=this._lView[pr];t!==null&&!e&&Gp(t,this._lView),U_(this._lView[we],this._lView)}attachToAppRef(e){if(this._attachedToViewContainer)throw new He(902,!1);this._appRef=e;let t=Xs(this._lView),i=this._lView[pr];i!==null&&!t&&i0(i,this._lView),Xc(this._lView)}};var fs=(()=>{class n{_declarationLView;_declarationTContainer;elementRef;static __NG_ELEMENT_ID__=JS;constructor(t,i,r){this._declarationLView=t,this._declarationTContainer=i,this.elementRef=r}get ssrId(){return this._declarationTContainer.tView?.ssrId||null}createEmbeddedView(t,i){return this.createEmbeddedViewImpl(t,i)}createEmbeddedViewImpl(t,i,r){let s=kl(this._declarationLView,this._declarationTContainer,t,{embeddedViewInjector:i,dehydratedView:r});return new no(s)}}return n})();function JS(){return Zp(Dn(),Ye())}function Zp(n,e){return n.type&4?new fs(e,n,ro(n,e)):null}function ya(n,e,t,i,r){let s=n.data[e];if(s===null)s=KS(n,e,t,i,r),Zv()&&(s.flags|=32);else if(s.type&64){s.type=t,s.value=i,s.attrs=r;let o=jv();s.injectorIndex=o===null?-1:o.injectorIndex}return Zs(s,!0),s}function KS(n,e,t,i,r){let s=Ch(),o=wh(),a=o?s:s&&s.parent,c=n.data[e]=eT(n,a,t,e,i,r);return QS(n,c,s,o),c}function QS(n,e,t,i){n.firstChild===null&&(n.firstChild=e),t!==null&&(i?t.child==null&&e.parent!==null&&(t.child=e):t.next===null&&(t.next=e,e.prev=t))}function eT(n,e,t,i,r,s){let o=e?e.injectorIndex:-1,a=0;return Wv()&&(a|=128),{type:t,index:i,insertBeforeIndex:null,injectorIndex:o,directiveStart:-1,directiveEnd:-1,directiveStylingLast:-1,componentOffset:-1,controlDirectiveIndex:-1,customControlIndex:-1,propertyBindings:null,flags:a,providerIndexes:0,value:r,attrs:s,mergedAttrs:null,localNames:null,initialInputs:null,inputs:null,hostDirectiveInputs:null,outputs:null,hostDirectiveOutputs:null,directiveToIndex:null,tView:null,next:null,prev:null,projectionNext:null,child:null,parent:e,projection:null,styles:null,stylesWithoutHost:null,residualStyles:void 0,classes:null,classesWithoutHost:null,residualClasses:void 0,classBindings:0,styleBindings:0}}function tT(n){let e=n[hh]??[],i=n[jt][wt],r=[];for(let s of e)s.data[p_]!==void 0?r.push(s):nT(s,i);n[hh]=r}function nT(n,e){let t=0,i=n.firstChild;if(i){let r=n.data[h_];for(;t<r;){let s=i.nextSibling;D_(e,i,!1),i=s,t++}}}var iT=()=>null,rT=()=>null;function cp(n,e){return iT(n,e)}function r0(n,e,t){return rT(n,e,t)}var s0=class{},Bl=class{},lp=class{resolveComponentFactory(e){throw new He(917,!1)}},Vl=class{static NULL=new lp},hs=class{},Hl=(()=>{class n{destroyNode=null;static __NG_ELEMENT_ID__=()=>sT()}return n})();function sT(){let n=Ye(),e=Dn(),t=si(e.index,n);return(Gi(t)?t:n)[wt]}var o0=(()=>{class n{static \u0275prov=at({token:n,providedIn:"root",factory:()=>null})}return n})();var dl={},up=class{injector;parentInjector;constructor(e,t){this.injector=e,this.parentInjector=t}get(e,t,i){let r=this.injector.get(e,dl,i);return r!==dl||t===dl?r:this.parentInjector.get(e,t,i)}};function xl(n,e,t){let i=t?n.styles:null,r=t?n.classes:null,s=0;if(e!==null)for(let o=0;o<e.length;o++){let a=e[o];if(typeof a=="number")s=a;else if(s==1)r=Jf(r,a);else if(s==2){let c=a,l=e[++o];i=Jf(i,c+": "+l+";")}}t?n.styles=i:n.stylesWithoutHost=i,t?n.classes=r:n.classesWithoutHost=r}function ao(n,e=0){let t=Ye();if(t===null)return et(n,e);let i=Dn();return r_(i,t,Cn(n),e)}function a0(n,e,t,i,r){let s=i===null?null:{"":-1},o=r(n,t);if(o!==null){let a=o,c=null,l=null;for(let u of o)if(u.resolveHostDirectives!==null){[a,c,l]=u.resolveHostDirectives(o);break}cT(n,e,t,a,s,c,l)}s!==null&&i!==null&&oT(t,i,s)}function oT(n,e,t){let i=n.localNames=[];for(let r=0;r<e.length;r+=2){let s=t[e[r+1]];if(s==null)throw new He(-301,!1);i.push(e[r],s)}}function aT(n,e,t){e.componentOffset=t,(n.components??=[]).push(e.index)}function cT(n,e,t,i,r,s,o){let a=i.length,c=null;for(let d=0;d<a;d++){let h=i[d];c===null&&as(h)&&(c=h,aT(n,t,d)),oE(e_(t,e),n,h.type)}pT(t,n.data.length,a),c?.viewProvidersResolver&&c.viewProvidersResolver(c);for(let d=0;d<a;d++){let h=i[d];h.providersResolver&&h.providersResolver(h)}let l=!1,u=!1,f=P_(n,e,a,null);a>0&&(t.directiveToIndex=new Map);for(let d=0;d<a;d++){let h=i[d];if(t.mergedAttrs=wl(t.mergedAttrs,h.hostAttrs),uT(n,t,e,f,h),hT(f,h,r),o!==null&&o.has(h)){let[x,m]=o.get(h);t.directiveToIndex.set(h.type,[f,x+t.directiveStart,m+t.directiveStart])}else(s===null||!s.has(h))&&t.directiveToIndex.set(h.type,f);h.contentQueries!==null&&(t.flags|=4),(h.hostBindings!==null||h.hostAttrs!==null||h.hostVars!==0)&&(t.flags|=64);let g=h.type.prototype;!l&&(g.ngOnChanges||g.ngOnInit||g.ngDoCheck)&&((n.preOrderHooks??=[]).push(t.index),l=!0),!u&&(g.ngOnChanges||g.ngDoCheck)&&((n.preOrderCheckHooks??=[]).push(t.index),u=!0),f++}lT(n,t,s)}function lT(n,e,t){for(let i=e.directiveStart;i<e.directiveEnd;i++){let r=n.data[i];if(t===null||!t.has(r))Ny(0,e,r,i),Ny(1,e,r,i),Ly(e,i,!1);else{let s=t.get(r);Py(0,e,s,i),Py(1,e,s,i),Ly(e,i,!0)}}}function Ny(n,e,t,i){let r=n===0?t.inputs:t.outputs;for(let s in r)if(r.hasOwnProperty(s)){let o;n===0?o=e.inputs??={}:o=e.outputs??={},o[s]??=[],o[s].push(i),c0(e,s)}}function Py(n,e,t,i){let r=n===0?t.inputs:t.outputs;for(let s in r)if(r.hasOwnProperty(s)){let o=r[s],a;n===0?a=e.hostDirectiveInputs??={}:a=e.hostDirectiveOutputs??={},a[o]??=[],a[o].push(i,s),c0(e,o)}}function c0(n,e){e==="class"?n.flags|=8:e==="style"&&(n.flags|=16)}function Ly(n,e,t){let{attrs:i,inputs:r,hostDirectiveInputs:s}=n;if(i===null||!t&&r===null||t&&s===null||kp(n)){n.initialInputs??=[],n.initialInputs.push(null);return}let o=null,a=0;for(;a<i.length;){let c=i[a];if(c===0){a+=4;continue}else if(c===5){a+=2;continue}else if(typeof c=="number")break;if(!t&&r.hasOwnProperty(c)){let l=r[c];for(let u of l)if(u===e){o??=[],o.push(c,i[a+1]);break}}else if(t&&s.hasOwnProperty(c)){let l=s[c];for(let u=0;u<l.length;u+=2)if(l[u]===e){o??=[],o.push(l[u+1],i[a+1]);break}}a+=2}n.initialInputs??=[],n.initialInputs.push(o)}function uT(n,e,t,i,r){n.data[i]=r;let s=r.factory||(r.factory=Zr(r.type,!0)),o=new aa(s,as(r),ao,null);n.blueprint[i]=o,t[i]=o,dT(n,e,i,P_(n,t,r.hostVars,qn),r)}function dT(n,e,t,i,r){let s=r.hostBindings;if(s){let o=n.hostBindingOpCodes;o===null&&(o=n.hostBindingOpCodes=[]);let a=~e.index;fT(o)!=a&&o.push(a),o.push(t,i,s)}}function fT(n){let e=n.length;for(;e>0;){let t=n[--e];if(typeof t=="number"&&t<0)return t}return 0}function hT(n,e,t){if(t){if(e.exportAs)for(let i=0;i<e.exportAs.length;i++)t[e.exportAs[i]]=n;as(e)&&(t[""]=n)}}function pT(n,e,t){n.flags|=1,n.directiveStart=e,n.directiveEnd=e+t,n.providerIndexes=e}function l0(n,e,t,i,r,s,o,a){let c=e[we],l=c.consts,u=$n(l,o),f=ya(c,n,t,i,u);return s&&a0(c,e,f,$n(l,a),r),f.mergedAttrs=wl(f.mergedAttrs,f.attrs),f.attrs!==null&&xl(f,f.attrs,!1),f.mergedAttrs!==null&&xl(f,f.mergedAttrs,!0),c.queries!==null&&c.queries.elementStart(c,f),f}function u0(n,e){Yy(n,e),ph(e)&&n.queries.elementEnd(e)}function mT(n,e,t,i,r,s){let o=e.consts,a=$n(o,r),c=ya(e,n,t,i,a);if(c.mergedAttrs=wl(c.mergedAttrs,c.attrs),s!=null){let l=$n(o,s);c.localNames=[];for(let u=0;u<l.length;u+=2)c.localNames.push(l[u],-1)}return c.attrs!==null&&xl(c,c.attrs,!1),c.mergedAttrs!==null&&xl(c,c.mergedAttrs,!0),e.queries!==null&&e.queries.elementStart(e,c),c}function gT(n,e,t){return n[e]=t}function bi(n,e,t){if(t===qn)return!1;let i=n[e];return Object.is(i,t)?!1:(n[e]=t,!0)}function vT(n,e,t,i){let r=bi(n,e,t);return bi(n,e+1,i)||r}function fl(n,e,t){return function i(r){let s=vr(n)?si(n.index,e):e;Yp(s,5);let o=e[kt],a=Fy(e,o,t,r),c=i.__ngNextListenerFn__;for(;c;)a=Fy(e,o,c,r)&&a,c=c.__ngNextListenerFn__;return a}}function Fy(n,e,t,i){let r=Ae(null);try{return yt(ct.OutputStart,e,t),t(i)!==!1}catch(s){return LS(n,s),!1}finally{yt(ct.OutputEnd,e,t),Ae(r)}}function d0(n,e,t,i,r,s,o,a){let c=ta(n),l=!1,u=null;if(!i&&c&&(u=_T(e,t,s,n.index)),u!==null){let f=u.__ngLastListenerFn__||u;f.__ngNextListenerFn__=o,u.__ngLastListenerFn__=o,l=!0}else{let f=ri(n,t),d=i?i(f):f;_E(t,d,s,a);let h=r.listen(d,s,a);if(!yT(s)){let g=i?x=>i(jn(x[n.index])):n.index;f0(g,e,t,s,a,h,!1)}}return l}function yT(n){return n.startsWith("animation")||n.startsWith("transition")}function _T(n,e,t,i){let r=n.cleanup;if(r!=null)for(let s=0;s<r.length-1;s+=2){let o=r[s];if(o===t&&r[s+1]===i){let a=e[$s],c=r[s+2];return a&&a.length>c?a[c]:null}typeof o=="string"&&(s+=2)}return null}function f0(n,e,t,i,r,s,o){let a=e.firstCreatePass?Mh(e):null,c=xh(t),l=c.length;c.push(r,s),a&&a.push(i,n,l,(l+1)*(o?-1:1))}function Oy(n,e,t,i,r,s){let o=e[t],a=e[we],l=a.data[t].outputs[i],f=o[l].subscribe(s);f0(n.index,a,e,r,s,f,!0)}var dp=Symbol("BINDING");function h0(n){return n.debugInfo?.className||n.type.name||null}var fp=class extends Vl{ngModule;constructor(e){super(),this.ngModule=e}resolveComponentFactory(e){let t=es(e);return new da(t,this.ngModule)}};function xT(n){return Object.keys(n).map(e=>{let[t,i,r]=n[e],s={propName:t,templateName:e,isSignal:(i&Pl.SignalBased)!==0};return r&&(s.transform=r),s})}function MT(n){return Object.keys(n).map(e=>({propName:n[e],templateName:e}))}function bT(n,e,t){let i=e instanceof Vn?e:e?.injector;return i&&n.getStandaloneInjector!==null&&(i=n.getStandaloneInjector(i)||i),i?new up(t,i):t}function ET(n){let e=n.get(hs,null);if(e===null)throw new He(407,!1);let t=n.get(o0,null),i=n.get(Kr,null),r=n.get(so,null,{optional:!0});return{rendererFactory:e,sanitizer:t,changeDetectionScheduler:i,ngReflect:!1,tracingService:r}}function ST(n,e){let t=p0(n);return C_(e,t,t==="svg"?Lv:t==="math"?Fv:null)}function p0(n){return(n.selectors[0][0]||"div").toLowerCase()}var da=class extends Bl{componentDef;ngModule;selector;componentType;ngContentSelectors;isBoundToModule;cachedInputs=null;cachedOutputs=null;get inputs(){return this.cachedInputs??=xT(this.componentDef.inputs),this.cachedInputs}get outputs(){return this.cachedOutputs??=MT(this.componentDef.outputs),this.cachedOutputs}constructor(e,t){super(),this.componentDef=e,this.ngModule=t,this.componentType=e.type,this.selector=KE(e.selectors),this.ngContentSelectors=e.ngContentSelectors??[],this.isBoundToModule=!!t}create(e,t,i,r,s,o){yt(ct.DynamicComponentStart);let a=Ae(null);try{let c=this.componentDef,l=bT(c,r||this.ngModule,e),u=ET(l),f=u.tracingService;return f&&f.componentCreate?f.componentCreate(h0(c),()=>this.createComponentRef(u,l,t,i,s,o)):this.createComponentRef(u,l,t,i,s,o)}finally{Ae(a)}}createComponentRef(e,t,i,r,s,o){let a=this.componentDef,c=TT(r,a,o,s),l=e.rendererFactory.createRenderer(null,a),u=r?bS(l,r,a.encapsulation,t):ST(a,l),f=o?.some(ky)||s?.some(g=>typeof g!="function"&&g.bindings.some(ky)),d=Bp(null,c,null,512|N_(a),null,null,e,l,t,null,v_(u,t,!0));d[Ht]=u,Jc(d);let h=null;try{let g=l0(Ht,d,2,"#host",()=>c.directiveRegistry,!0,0);I_(l,u,g),to(u,d),$p(c,d,g),__(c,g,d),u0(c,g),i!==void 0&&wT(g,this.ngContentSelectors,i),h=si(g.index,d),d[kt]=h[kt],Xp(c,d,null)}catch(g){throw h!==null&&qh(h),qh(d),g}finally{yt(ct.DynamicComponentEnd),Kc()}return new Ml(this.componentType,d,!!f)}};function TT(n,e,t,i){let r=n?["ng-version","21.2.8"]:QE(e.selectors[0]),s=null,o=null,a=0;if(t)for(let u of t)a+=u[dp].requiredVars,u.create&&(u.targetIdx=0,(s??=[]).push(u)),u.update&&(u.targetIdx=0,(o??=[]).push(u));if(i)for(let u=0;u<i.length;u++){let f=i[u];if(typeof f!="function")for(let d of f.bindings){a+=d[dp].requiredVars;let h=u+1;d.create&&(d.targetIdx=h,(s??=[]).push(d)),d.update&&(d.targetIdx=h,(o??=[]).push(d))}}let c=[e];if(i)for(let u of i){let f=typeof u=="function"?u:u.type,d=nh(f);c.push(d)}return Up(0,null,CT(s,o),1,a,c,null,null,null,[r],null)}function CT(n,e){return!n&&!e?null:t=>{if(t&1&&n)for(let i of n)i.create();if(t&2&&e)for(let i of e)i.update()}}function ky(n){let e=n[dp].kind;return e==="input"||e==="twoWay"}var Ml=class extends s0{_rootLView;_hasInputBindings;instance;hostView;changeDetectorRef;componentType;location;previousInputValues=null;_tNode;constructor(e,t,i){super(),this._rootLView=t,this._hasInputBindings=i,this._tNode=$c(t[we],Ht),this.location=ro(this._tNode,t),this.instance=si(this._tNode.index,t)[kt],this.hostView=this.changeDetectorRef=new no(t,void 0),this.componentType=e}setInput(e,t){this._hasInputBindings;let i=this._tNode;if(this.previousInputValues??=new Map,this.previousInputValues.has(e)&&Object.is(this.previousInputValues.get(e),t))return;let r=this._rootLView,s=qp(i,r[we],r,e,t);this.previousInputValues.set(e,t);let o=si(i.index,r);Yp(o,1)}get injector(){return new us(this._tNode,this._rootLView)}destroy(){this.hostView.destroy()}onDestroy(e){this.hostView.onDestroy(e)}};function wT(n,e,t){let i=n.projection=[];for(let r=0;r<e.length;r++){let s=t[r];i.push(s!=null&&s.length?Array.from(s):null)}}var co=(()=>{class n{static __NG_ELEMENT_ID__=DT}return n})();function DT(){let n=Dn();return m0(n,Ye())}var hp=class n extends co{_lContainer;_hostTNode;_hostLView;constructor(e,t,i){super(),this._lContainer=e,this._hostTNode=t,this._hostLView=i}get element(){return ro(this._hostTNode,this._hostLView)}get injector(){return new us(this._hostTNode,this._hostLView)}get parentInjector(){let e=Dp(this._hostTNode,this._hostLView);if(Jy(e)){let t=ml(e,this._hostLView),i=pl(e),r=t[we].data[i+8];return new us(r,t)}else return new us(null,this._hostLView)}clear(){for(;this.length>0;)this.remove(this.length-1)}get(e){let t=Uy(this._lContainer);return t!==null&&t[e]||null}get length(){return this._lContainer.length-At}createEmbeddedView(e,t,i){let r,s;typeof i=="number"?r=i:i!=null&&(r=i.index,s=i.injector);let o=cp(this._lContainer,e.ssrId),a=e.createEmbeddedViewImpl(t||{},s,o);return this.insertImpl(a,r,ca(this._hostTNode,o)),a}createComponent(e,t,i,r,s,o,a){let c=e&&!Wb(e),l;if(c)l=t;else{let m=t||{};l=m.index,i=m.injector,r=m.projectableNodes,s=m.environmentInjector||m.ngModuleRef,o=m.directives,a=m.bindings}let u=c?e:new da(es(e)),f=i||this.parentInjector;if(!s&&u.ngModule==null){let p=(c?f:this.parentInjector).get(Vn,null);p&&(s=p)}let d=es(u.componentType??{}),h=cp(this._lContainer,d?.id??null),g=h?.firstChild??null,x=u.create(f,r,g,s,o,a);return this.insertImpl(x.hostView,l,ca(this._hostTNode,h)),x}insert(e,t){return this.insertImpl(e,t,!0)}insertImpl(e,t,i){let r=e._lView;if(Uv(r)){let a=this.indexOf(e);if(a!==-1)this.detach(a);else{let c=r[jt],l=new n(c,c[Gn],c[jt]);l.detach(l.indexOf(e))}}let s=this._adjustIndex(t),o=this._lContainer;return Ul(o,r,s,i),e.attachToViewContainerRef(),oh(zh(o),s,e),e}move(e,t){return this.insert(e,t)}indexOf(e){let t=Uy(this._lContainer);return t!==null?t.indexOf(e):-1}remove(e){let t=this._adjustIndex(e,-1),i=ua(this._lContainer,t);i&&(Zo(zh(this._lContainer),t),Ll(i[we],i))}detach(e){let t=this._adjustIndex(e,-1),i=ua(this._lContainer,t);return i&&Zo(zh(this._lContainer),t)!=null?new no(i):null}_adjustIndex(e,t=0){return e??this.length+t}};function Uy(n){return n[ea]}function zh(n){return n[ea]||(n[ea]=[])}function m0(n,e){let t,i=e[n.index];return ii(i)?t=i:(t=e0(i,e,null,n),e[n.index]=t,Vp(e,t)),AT(t,e,n,i),new hp(t,n,e)}function IT(n,e){let t=n[wt],i=t.createComment(""),r=ri(e,n),s=t.parentNode(r);return _l(t,s,i,t.nextSibling(r),!1),i}var AT=PT,RT=()=>!1;function NT(n,e,t){return RT(n,e,t)}function PT(n,e,t,i){if(n[gr])return;let r;t.type&8?r=jn(i):r=IT(e,t),n[gr]=r}var pp=class n{queryList;matches=null;constructor(e){this.queryList=e}clone(){return new n(this.queryList)}setDirty(){this.queryList.setDirty()}},mp=class n{queries;constructor(e=[]){this.queries=e}createEmbeddedView(e){let t=e.queries;if(t!==null){let i=e.contentQueries!==null?e.contentQueries[0]:t.length,r=[];for(let s=0;s<i;s++){let o=t.getByIndex(s),a=this.queries[o.indexInDeclarationView];r.push(a.clone())}return new n(r)}return null}insertView(e){this.dirtyQueriesWithMatches(e)}detachView(e){this.dirtyQueriesWithMatches(e)}finishViewCreation(e){this.dirtyQueriesWithMatches(e)}dirtyQueriesWithMatches(e){for(let t=0;t<this.queries.length;t++)Jp(e,t).matches!==null&&this.queries[t].setDirty()}},gp=class{flags;read;predicate;constructor(e,t,i=null){this.flags=t,this.read=i,typeof e=="string"?this.predicate=HT(e):this.predicate=e}},vp=class n{queries;constructor(e=[]){this.queries=e}elementStart(e,t){for(let i=0;i<this.queries.length;i++)this.queries[i].elementStart(e,t)}elementEnd(e){for(let t=0;t<this.queries.length;t++)this.queries[t].elementEnd(e)}embeddedTView(e){let t=null;for(let i=0;i<this.length;i++){let r=t!==null?t.length:0,s=this.getByIndex(i).embeddedTView(e,r);s&&(s.indexInDeclarationView=i,t!==null?t.push(s):t=[s])}return t!==null?new n(t):null}template(e,t){for(let i=0;i<this.queries.length;i++)this.queries[i].template(e,t)}getByIndex(e){return this.queries[e]}get length(){return this.queries.length}track(e){this.queries.push(e)}},yp=class n{metadata;matches=null;indexInDeclarationView=-1;crossesNgTemplate=!1;_declarationNodeIndex;_appliesToNextNode=!0;constructor(e,t=-1){this.metadata=e,this._declarationNodeIndex=t}elementStart(e,t){this.isApplyingToNode(t)&&this.matchTNode(e,t)}elementEnd(e){this._declarationNodeIndex===e.index&&(this._appliesToNextNode=!1)}template(e,t){this.elementStart(e,t)}embeddedTView(e,t){return this.isApplyingToNode(e)?(this.crossesNgTemplate=!0,this.addMatch(-e.index,t),new n(this.metadata)):null}isApplyingToNode(e){if(this._appliesToNextNode&&(this.metadata.flags&1)!==1){let t=this._declarationNodeIndex,i=e.parent;for(;i!==null&&i.type&8&&i.index!==t;)i=i.parent;return t===(i!==null?i.index:-1)}return this._appliesToNextNode}matchTNode(e,t){let i=this.metadata.predicate;if(Array.isArray(i))for(let r=0;r<i.length;r++){let s=i[r];this.matchTNodeWithReadOption(e,t,LT(t,s)),this.matchTNodeWithReadOption(e,t,ul(t,e,s,!1,!1))}else i===fs?t.type&4&&this.matchTNodeWithReadOption(e,t,-1):this.matchTNodeWithReadOption(e,t,ul(t,e,i,!1,!1))}matchTNodeWithReadOption(e,t,i){if(i!==null){let r=this.metadata.read;if(r!==null)if(r===br||r===co||r===fs&&t.type&4)this.addMatch(t.index,-2);else{let s=ul(t,e,r,!1,!1);s!==null&&this.addMatch(t.index,s)}else this.addMatch(t.index,i)}}addMatch(e,t){this.matches===null?this.matches=[e,t]:this.matches.push(e,t)}};function LT(n,e){let t=n.localNames;if(t!==null){for(let i=0;i<t.length;i+=2)if(t[i]===e)return t[i+1]}return null}function FT(n,e){return n.type&11?ro(n,e):n.type&4?Zp(n,e):null}function OT(n,e,t,i){return t===-1?FT(e,n):t===-2?kT(n,e,i):vl(n,n[we],t,e)}function kT(n,e,t){if(t===br)return ro(e,n);if(t===fs)return Zp(e,n);if(t===co)return m0(e,n)}function g0(n,e,t,i){let r=e[_i].queries[i];if(r.matches===null){let s=n.data,o=t.matches,a=[];for(let c=0;o!==null&&c<o.length;c+=2){let l=o[c];if(l<0)a.push(null);else{let u=s[l];a.push(OT(e,u,o[c+1],t.metadata.read))}}r.matches=a}return r.matches}function _p(n,e,t,i){let r=n.queries.getByIndex(t),s=r.matches;if(s!==null){let o=g0(n,e,r,t);for(let a=0;a<s.length;a+=2){let c=s[a];if(c>0)i.push(o[a/2]);else{let l=s[a+1],u=e[-c];for(let f=At;f<u.length;f++){let d=u[f];d[pr]===d[jt]&&_p(d[we],d,l,i)}if(u[os]!==null){let f=u[os];for(let d=0;d<f.length;d++){let h=f[d];_p(h[we],h,l,i)}}}}}return i}function UT(n,e){return n[_i].queries[e].queryList}function BT(n,e,t){let i=new yl((t&4)===4);return Hv(n,e,i,i.destroy),(e[_i]??=new mp).queries.push(new pp(i))-1}function VT(n,e,t){let i=nn();return i.firstCreatePass&&(zT(i,new gp(n,e,t),-1),(e&2)===2&&(i.staticViewQueries=!0)),BT(i,Ye(),e)}function HT(n){return n.split(",").map(e=>e.trim())}function zT(n,e,t){n.queries===null&&(n.queries=new vp),n.queries.track(new yp(e,t))}function Jp(n,e){return n.queries.getByIndex(e)}function GT(n,e){let t=n[we],i=Jp(t,e);return i.crossesNgTemplate?_p(t,n,e,[]):g0(t,n,i,e)}var fa=class{};var ha=class extends fa{injector;componentFactoryResolver=new fp(this);instance=null;constructor(e){super();let t=new Jr([...e.providers,{provide:fa,useValue:this},{provide:Vl,useValue:this.componentFactoryResolver}],e.parent||Qo(),e.debugName,new Set(["environment"]));this.injector=t,e.runEnvironmentInitializers&&t.resolveInjectorInitializers()}destroy(){this.injector.destroy()}onDestroy(e){this.injector.onDestroy(e)}};function v0(n,e,t=null){return new ha({providers:n,parent:e,debugName:t,runEnvironmentInitializers:!0}).injector}var WT=(()=>{class n{_injector;cachedInjectors=new Map;constructor(t){this._injector=t}getOrCreateStandaloneInjector(t){if(!t.standalone)return null;if(!this.cachedInjectors.has(t)){let i=lh(!1,t.type),r=i.length>0?v0([i],this._injector,""):null;this.cachedInjectors.set(t,r)}return this.cachedInjectors.get(t)}ngOnDestroy(){try{for(let t of this.cachedInjectors.values())t!==null&&t.destroy()}finally{this.cachedInjectors.clear()}}static \u0275prov=at({token:n,providedIn:"environment",factory:()=>new n(et(Vn))})}return n})();function Mt(n){return Cl(()=>{let e=y0(n),t=Wt(It({},e),{decls:n.decls,vars:n.vars,template:n.template,consts:n.consts||null,ngContentSelectors:n.ngContentSelectors,onPush:n.changeDetection===Ip.OnPush,directiveDefs:null,pipeDefs:null,dependencies:e.standalone&&n.dependencies||null,getStandaloneInjector:e.standalone?r=>r.get(WT).getOrCreateStandaloneInjector(t):null,getExternalStyles:null,signals:n.signals??!1,data:n.data||{},encapsulation:n.encapsulation||ai.Emulated,styles:n.styles||Bn,_:null,schemas:n.schemas||null,tView:null,id:""});e.standalone&&oo("NgStandalone"),_0(t);let i=n.dependencies;return t.directiveDefs=By(i,jT),t.pipeDefs=By(i,_v),t.id=XT(t),t})}function jT(n){return es(n)||nh(n)}function _a(n){return Cl(()=>({type:n.type,bootstrap:n.bootstrap||Bn,declarations:n.declarations||Bn,imports:n.imports||Bn,exports:n.exports||Bn,transitiveCompileScopes:null,schemas:n.schemas||null,id:n.id||null}))}function $T(n,e){if(n==null)return ns;let t={};for(let i in n)if(n.hasOwnProperty(i)){let r=n[i],s,o,a,c;Array.isArray(r)?(a=r[0],s=r[1],o=r[2]??s,c=r[3]||null):(s=r,o=r,a=Pl.None,c=null),t[s]=[i,a,c],e[s]=o}return t}function qT(n){if(n==null)return ns;let e={};for(let t in n)n.hasOwnProperty(t)&&(e[n[t]]=t);return e}function xa(n){return Cl(()=>{let e=y0(n);return _0(e),e})}function zl(n){return{type:n.type,name:n.name,factory:null,pure:n.pure!==!1,standalone:n.standalone??!0,onDestroy:n.type.prototype.ngOnDestroy||null}}function y0(n){let e={};return{type:n.type,providersResolver:null,viewProvidersResolver:null,factory:null,hostBindings:n.hostBindings||null,hostVars:n.hostVars||0,hostAttrs:n.hostAttrs||null,contentQueries:n.contentQueries||null,declaredInputs:e,inputConfig:n.inputs||ns,exportAs:n.exportAs||null,standalone:n.standalone??!0,signals:n.signals===!0,selectors:n.selectors||Bn,viewQuery:n.viewQuery||null,features:n.features||null,setInput:null,resolveHostDirectives:null,hostDirectives:null,controlDef:null,inputs:$T(n.inputs,e),outputs:qT(n.outputs),debugInfo:null}}function _0(n){n.features?.forEach(e=>e(n))}function By(n,e){return n?()=>{let t=typeof n=="function"?n():n,i=[];for(let r of t){let s=e(r);s!==null&&i.push(s)}return i}:null}function XT(n){let e=0,t=typeof n.consts=="function"?"":n.consts,i=[n.selectors,n.ngContentSelectors,n.hostVars,n.hostAttrs,t,n.vars,n.decls,n.encapsulation,n.standalone,n.signals,n.exportAs,JSON.stringify(n.inputs),JSON.stringify(n.outputs),Object.getOwnPropertyNames(n.type.prototype),!!n.contentQueries,!!n.viewQuery];for(let s of i.join("|"))e=Math.imul(31,e)+s.charCodeAt(0)<<0;return e+=2147483648,"c"+e}function x0(n,e,t,i,r,s,o,a){if(t.firstCreatePass){n.mergedAttrs=wl(n.mergedAttrs,n.attrs);let u=n.tView=Up(2,n,r,s,o,t.directiveRegistry,t.pipeRegistry,null,t.schemas,t.consts,null);t.queries!==null&&(t.queries.template(t,n),u.queries=t.queries.embeddedTView(n))}a&&(n.flags|=a),Zs(n,!1);let c=ZT(t,e,n,i);el()&&Wp(t,e,c,n),to(c,e);let l=e0(c,e,c,n);e[i+Ht]=l,Vp(e,l),NT(l,n,e)}function YT(n,e,t,i,r,s,o,a,c,l,u){let f=t+Ht,d;return e.firstCreatePass?(d=ya(e,f,4,o||null,a||null),Eh()&&a0(e,n,d,$n(e.consts,l),z_),Yy(e,d)):d=e.data[f],x0(d,n,e,t,i,r,s,c),ta(d)&&$p(e,n,d),l!=null&&Ol(n,d,u),d}function bl(n,e,t,i,r,s,o,a,c,l,u){let f=t+Ht,d;if(e.firstCreatePass){if(d=ya(e,f,4,o||null,a||null),l!=null){let h=$n(e.consts,l);d.localNames=[];for(let g=0;g<h.length;g+=2)d.localNames.push(h[g],-1)}}else d=e.data[f];return x0(d,n,e,t,i,r,s,c),l!=null&&Ol(n,d,u),d}function Gl(n,e,t,i,r,s,o,a){let c=Ye(),l=nn(),u=$n(l.consts,s);return YT(c,l,n,e,t,i,r,u,void 0,o,a),Gl}var ZT=JT;function JT(n,e,t,i){return tl(!0),e[wt].createComment("")}var Kp=new Qe("");function Wl(n){return!!n&&typeof n.then=="function"}function Qp(n){return!!n&&typeof n.subscribe=="function"}var M0=new Qe("");var em=(()=>{class n{resolve;reject;initialized=!1;done=!1;donePromise=new Promise((t,i)=>{this.resolve=t,this.reject=i});appInits=pe(M0,{optional:!0})??[];injector=pe(ei);constructor(){}runInitializers(){if(this.initialized)return;let t=[];for(let r of this.appInits){let s=Gc(this.injector,r);if(Wl(s))t.push(s);else if(Qp(s)){let o=new Promise((a,c)=>{s.subscribe({complete:a,error:c})});t.push(o)}}let i=()=>{this.done=!0,this.resolve()};Promise.all(t).then(()=>{i()}).catch(r=>{this.reject(r)}),t.length===0&&i(),this.initialized=!0}static \u0275fac=function(i){return new(i||n)};static \u0275prov=at({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})(),b0=new Qe("");function E0(){Tf(()=>{let n="";throw new He(600,n)})}function S0(n){return n.isBoundToModule}var KT=10;var Ma=(()=>{class n{_runningTick=!1;_destroyed=!1;_destroyListeners=[];_views=[];internalErrorHandler=pe(Mr);afterRenderManager=pe(F_);zonelessEnabled=pe(ia);rootEffectScheduler=pe(il);dirtyFlags=0;tracingSnapshot=null;allTestViews=new Set;autoDetectTestViews=new Set;includeAllTestViews=!1;afterTick=new ki;get allViews(){return[...(this.includeAllTestViews?this.allTestViews:this.autoDetectTestViews).keys(),...this._views]}get destroyed(){return this._destroyed}componentTypes=[];components=[];internalPendingTask=pe(Ks);get isStable(){return this.internalPendingTask.hasPendingTasksObservable.pipe(Of(t=>!t))}constructor(){pe(so,{optional:!0})}whenStable(){let t;return new Promise(i=>{t=this.isStable.subscribe({next:r=>{r&&i()}})}).finally(()=>{t.unsubscribe()})}_injector=pe(Vn);_rendererFactory=null;get injector(){return this._injector}bootstrap(t,i){return this.bootstrapImpl(t,i)}bootstrapImpl(t,i,r=ei.NULL){return this._injector.get(Hn).run(()=>{yt(ct.BootstrapComponentStart);let o=t instanceof Bl;if(!this._injector.get(em).done){let g="";throw new He(405,g)}let c;o?c=t:c=this._injector.get(Vl).resolveComponentFactory(t),this.componentTypes.push(c.componentType);let l=S0(c)?void 0:this._injector.get(fa),u=i||c.selector,f=c.create(r,[],u,l),d=f.location.nativeElement,h=f.injector.get(Kp,null);return h?.registerApplication(d),f.onDestroy(()=>{this.detachView(f.hostView),oa(this.components,f),h?.unregisterApplication(d)}),this._loadComponent(f),yt(ct.BootstrapComponentEnd,f),f})}tick(){this.zonelessEnabled||(this.dirtyFlags|=1),this._tick()}_tick(){yt(ct.ChangeDetectionStart),this.tracingSnapshot!==null?this.tracingSnapshot.run(zp.CHANGE_DETECTION,this.tickImpl):this.tickImpl()}tickImpl=()=>{if(this._runningTick)throw yt(ct.ChangeDetectionEnd),new He(101,!1);let t=Ae(null);try{this._runningTick=!0,this.synchronize()}finally{this._runningTick=!1,this.tracingSnapshot?.dispose(),this.tracingSnapshot=null,Ae(t),this.afterTick.next(),yt(ct.ChangeDetectionEnd)}};synchronize(){this._rendererFactory===null&&!this._injector.destroyed&&(this._rendererFactory=this._injector.get(hs,null,{optional:!0}));let t=0;for(;this.dirtyFlags!==0&&t++<KT;){yt(ct.ChangeDetectionSyncStart);try{this.synchronizeOnce()}finally{yt(ct.ChangeDetectionSyncEnd)}}}synchronizeOnce(){this.dirtyFlags&16&&(this.dirtyFlags&=-17,this.rootEffectScheduler.flush());let t=!1;if(this.dirtyFlags&7){let i=!!(this.dirtyFlags&1);this.dirtyFlags&=-8,this.dirtyFlags|=8;for(let{_lView:r}of this.allViews){if(!i&&!na(r))continue;let s=i&&!this.zonelessEnabled?0:1;Z_(r,s),t=!0}if(this.dirtyFlags&=-5,this.syncDirtyFlagsWithViews(),this.dirtyFlags&23)return}t||(this._rendererFactory?.begin?.(),this._rendererFactory?.end?.()),this.dirtyFlags&8&&(this.dirtyFlags&=-9,this.afterRenderManager.execute()),this.syncDirtyFlagsWithViews()}syncDirtyFlagsWithViews(){if(this.allViews.some(({_lView:t})=>na(t))){this.dirtyFlags|=2;return}else this.dirtyFlags&=-8}attachView(t){let i=t;this._views.push(i),i.attachToAppRef(this)}detachView(t){let i=t;oa(this._views,i),i.detachFromAppRef()}_loadComponent(t){this.attachView(t.hostView);try{this.tick()}catch(r){this.internalErrorHandler(r)}this.components.push(t),this._injector.get(b0,[]).forEach(r=>r(t))}ngOnDestroy(){if(!this._destroyed)try{this._destroyListeners.forEach(t=>t()),this._views.slice().forEach(t=>t.destroy())}finally{this._destroyed=!0,this._views=[],this._destroyListeners=[]}}onDestroy(t){return this._destroyListeners.push(t),()=>oa(this._destroyListeners,t)}destroy(){if(this._destroyed)throw new He(406,!1);let t=this._injector;t.destroy&&!t.destroyed&&t.destroy()}get viewCount(){return this._views.length}static \u0275fac=function(i){return new(i||n)};static \u0275prov=at({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();function oa(n,e){let t=n.indexOf(e);t>-1&&n.splice(t,1)}function Yi(n,e,t,i){let r=Ye(),s=ls();if(bi(r,s,e)){let o=nn(),a=Qc();RS(a,r,n,e,t,i)}return Yi}var xp=class{destroy(e){}updateValue(e,t){}swap(e,t){let i=Math.min(e,t),r=Math.max(e,t),s=this.detach(r);if(r-i>1){let o=this.detach(i);this.attach(i,s),this.attach(r,o)}else this.attach(i,s)}move(e,t){this.attach(t,this.detach(e))}};function Gh(n,e,t,i,r){return n===t&&Object.is(e,i)?1:Object.is(r(n,e),r(t,i))?-1:0}function QT(n,e,t,i){let r,s,o=0,a=n.length-1,c=void 0;if(Array.isArray(e)){Ae(i);let l=e.length-1;for(Ae(null);o<=a&&o<=l;){let u=n.at(o),f=e[o],d=Gh(o,u,o,f,t);if(d!==0){d<0&&n.updateValue(o,f),o++;continue}let h=n.at(a),g=e[l],x=Gh(a,h,l,g,t);if(x!==0){x<0&&n.updateValue(a,g),a--,l--;continue}let m=t(o,u),p=t(a,h),M=t(o,f);if(Object.is(M,p)){let T=t(l,g);Object.is(T,m)?(n.swap(o,a),n.updateValue(a,g),l--,a--):n.move(a,o),n.updateValue(o,f),o++;continue}if(r??=new El,s??=Hy(n,o,a,t),Mp(n,r,o,M))n.updateValue(o,f),o++,a++;else if(s.has(M))r.set(m,n.detach(o)),a--;else{let T=n.create(o,e[o]);n.attach(o,T),o++,a++}}for(;o<=l;)Vy(n,r,t,o,e[o]),o++}else if(e!=null){Ae(i);let l=e[Symbol.iterator]();Ae(null);let u=l.next();for(;!u.done&&o<=a;){let f=n.at(o),d=u.value,h=Gh(o,f,o,d,t);if(h!==0)h<0&&n.updateValue(o,d),o++,u=l.next();else{r??=new El,s??=Hy(n,o,a,t);let g=t(o,d);if(Mp(n,r,o,g))n.updateValue(o,d),o++,a++,u=l.next();else if(!s.has(g))n.attach(o,n.create(o,d)),o++,a++,u=l.next();else{let x=t(o,f);r.set(x,n.detach(o)),a--}}}for(;!u.done;)Vy(n,r,t,n.length,u.value),u=l.next()}for(;o<=a;)n.destroy(n.detach(a--));r?.forEach(l=>{n.destroy(l)})}function Mp(n,e,t,i){return e!==void 0&&e.has(i)?(n.attach(t,e.get(i)),e.delete(i),!0):!1}function Vy(n,e,t,i,r){if(Mp(n,e,i,t(i,r)))n.updateValue(i,r);else{let s=n.create(i,r);n.attach(i,s)}}function Hy(n,e,t,i){let r=new Set;for(let s=e;s<=t;s++)r.add(i(s,n.at(s)));return r}var El=class{kvMap=new Map;_vMap=void 0;has(e){return this.kvMap.has(e)}delete(e){if(!this.has(e))return!1;let t=this.kvMap.get(e);return this._vMap!==void 0&&this._vMap.has(t)?(this.kvMap.set(e,this._vMap.get(t)),this._vMap.delete(t)):this.kvMap.delete(e),!0}get(e){return this.kvMap.get(e)}set(e,t){if(this.kvMap.has(e)){let i=this.kvMap.get(e);this._vMap===void 0&&(this._vMap=new Map);let r=this._vMap;for(;r.has(i);)i=r.get(i);r.set(i,t)}else this.kvMap.set(e,t)}forEach(e){for(let[t,i]of this.kvMap)if(e(i,t),this._vMap!==void 0){let r=this._vMap;for(;r.has(i);)i=r.get(i),e(i,t)}}};function ci(n,e,t,i,r,s,o,a){oo("NgControlFlow");let c=Ye(),l=nn(),u=$n(l.consts,s);return bl(c,l,n,e,t,i,r,u,256,o,a),tm}function tm(n,e,t,i,r,s,o,a){oo("NgControlFlow");let c=Ye(),l=nn(),u=$n(l.consts,s);return bl(c,l,n,e,t,i,r,u,512,o,a),tm}function li(n,e){oo("NgControlFlow");let t=Ye(),i=ls(),r=t[i]!==qn?t[i]:-1,s=r!==-1?Sl(t,Ht+r):void 0,o=0;if(bi(t,i,n)){let a=Ae(null);try{if(s!==void 0&&n0(s,o),n!==-1){let c=Ht+n,l=Sl(t,c),u=Tp(t[we],c),f=r0(l,u,t),d=kl(t,u,e,{dehydratedView:f});Ul(l,d,o,ca(u,f))}}finally{Ae(a)}}else if(s!==void 0){let a=t0(s,o);a!==void 0&&(a[kt]=e)}}var bp=class{lContainer;$implicit;$index;constructor(e,t,i){this.lContainer=e,this.$implicit=t,this.$index=i}get $count(){return this.lContainer.length-At}};function Zi(n,e){return e}var Ep=class{hasEmptyBlock;trackByFn;liveCollection;constructor(e,t,i){this.hasEmptyBlock=e,this.trackByFn=t,this.liveCollection=i}};function Jt(n,e,t,i,r,s,o,a,c,l,u,f,d){oo("NgControlFlow");let h=Ye(),g=nn(),x=c!==void 0,m=Ye(),p=a?o.bind(m[Wn][kt]):o,M=new Ep(x,p);m[Ht+n]=M,bl(h,g,n+1,e,t,i,r,$n(g.consts,s),256),x&&bl(h,g,n+2,c,l,u,f,$n(g.consts,d),512)}var Sp=class extends xp{lContainer;hostLView;templateTNode;operationsCounter=void 0;needsIndexUpdate=!1;constructor(e,t,i){super(),this.lContainer=e,this.hostLView=t,this.templateTNode=i}get length(){return this.lContainer.length-At}at(e){return this.getLView(e)[kt].$implicit}attach(e,t){let i=t[js];this.needsIndexUpdate||=e!==this.length,Ul(this.lContainer,t,e,ca(this.templateTNode,i)),eC(this.lContainer,e)}detach(e){return this.needsIndexUpdate||=e!==this.length-1,tC(this.lContainer,e),nC(this.lContainer,e)}create(e,t){let i=cp(this.lContainer,this.templateTNode.tView.ssrId);return kl(this.hostLView,this.templateTNode,new bp(this.lContainer,t,e),{dehydratedView:i})}destroy(e){Ll(e[we],e)}updateValue(e,t){this.getLView(e)[kt].$implicit=t}reset(){this.needsIndexUpdate=!1}updateIndexes(){if(this.needsIndexUpdate)for(let e=0;e<this.length;e++)this.getLView(e)[kt].$index=e}getLView(e){return iC(this.lContainer,e)}};function Kt(n){let e=Ae(null),t=Wi();try{let i=Ye(),r=i[we],s=i[t],o=t+1,a=Sl(i,o);if(s.liveCollection===void 0){let l=Tp(r,o);s.liveCollection=new Sp(a,i,l)}else s.liveCollection.reset();let c=s.liveCollection;if(QT(c,n,s.trackByFn,e),c.updateIndexes(),s.hasEmptyBlock){let l=ls(),u=c.length===0;if(bi(i,l,u)){let f=t+2,d=Sl(i,f);if(u){let h=Tp(r,f),g=r0(d,h,i),x=kl(i,h,void 0,{dehydratedView:g});Ul(d,x,0,ca(h,g))}else r.firstUpdatePass&&tT(d),n0(d,0)}}}finally{Ae(e)}}function Sl(n,e){return n[e]}function eC(n,e){if(n.length<=At)return;let t=At+e,i=n[t],r=i?i[mr]:void 0;if(i&&r&&r.detachedLeaveAnimationFns&&r.detachedLeaveAnimationFns.length>0){let s=i[Hi];oS(s,r),ds.delete(i[zi]),r.detachedLeaveAnimationFns=void 0}}function tC(n,e){if(n.length<=At)return;let t=At+e,i=n[t],r=i?i[mr]:void 0;r&&r.leave&&r.leave.size>0&&(r.detachedLeaveAnimationFns=[])}function nC(n,e){return ua(n,e)}function iC(n,e){return t0(n,e)}function Tp(n,e){return $c(n,e)}function Ze(n,e,t){let i=Ye(),r=ls();if(bi(i,r,e)){let s=nn(),o=Qc();CS(o,i,n,e,i[wt],t)}return Ze}function zy(n,e,t,i,r){qp(e,n,t,r?"class":"style",i)}function De(n,e,t,i){let r=Ye(),s=r[we],o=n+Ht,a=s.firstCreatePass?l0(o,r,2,e,z_,Eh(),t,i):s.data[o];if(vr(a)){let c=r[ni].tracingService;if(c&&c.componentCreate){let l=s.data[a.directiveStart+a.componentOffset];return c.componentCreate(h0(l),()=>(Gy(n,e,r,a,i),De))}}return Gy(n,e,r,a,i),De}function Gy(n,e,t,i,r){if(G_(i,t,n,e,T0),ta(i)){let s=t[we];$p(s,t,i),__(s,i,t)}r!=null&&Ol(t,i)}function Pe(){let n=nn(),e=Dn(),t=W_(e);return n.firstCreatePass&&u0(n,t),Sh(t)&&Th(),bh(),t.classesWithoutHost!=null&&Jb(t)&&zy(n,t,Ye(),t.classesWithoutHost,!0),t.stylesWithoutHost!=null&&Kb(t)&&zy(n,t,Ye(),t.stylesWithoutHost,!1),Pe}function Je(n,e,t,i){return De(n,e,t,i),Pe(),Je}function Ut(n,e,t,i){let r=Ye(),s=r[we],o=n+Ht,a=s.firstCreatePass?mT(o,s,2,e,t,i):s.data[o];return G_(a,r,n,e,T0),i!=null&&Ol(r,a),Ut}function $t(){let n=Dn(),e=W_(n);return Sh(e)&&Th(),bh(),$t}function zt(n,e,t,i){return Ut(n,e,t,i),$t(),zt}var T0=(n,e,t,i,r)=>(tl(!0),C_(e[wt],i,ry()));function lo(){return Ye()}function rn(n,e,t){let i=Ye(),r=ls();if(bi(i,r,e)){let s=nn(),o=Qc();H_(o,i,n,e,i[wt],t)}return rn}var ba="en-US";var rC=ba;function C0(n){typeof n=="string"&&(rC=n.toLowerCase().replace(/_/g,"-"))}function uo(n,e,t){let i=Ye(),r=nn(),s=Dn();return sC(r,i,i[wt],s,n,e,t),uo}function Er(n,e,t){let i=Ye(),r=nn(),s=Dn();return(s.type&3||t)&&d0(s,r,i,t,i[wt],n,e,fl(s,i,e)),Er}function sC(n,e,t,i,r,s,o){let a=!0,c=null;if((i.type&3||o)&&(c??=fl(i,e,s),d0(i,n,e,o,t,r,s,c)&&(a=!1)),a){let l=i.outputs?.[r],u=i.hostDirectiveOutputs?.[r];if(u&&u.length)for(let f=0;f<u.length;f+=2){let d=u[f],h=u[f+1];c??=fl(i,e,s),Oy(i,e,d,h,r,c)}if(l&&l.length)for(let f of l)c??=fl(i,e,s),Oy(i,e,f,r,r,c)}}function En(n=1){return iy(n)}function jl(n,e,t){return VT(n,e,t),jl}function nm(n){let e=Ye(),t=nn(),i=Ah();Zc(i+1);let r=Jp(t,i);if(n.dirty&&kv(e)===((r.metadata.flags&2)===2)){if(r.matches===null)n.reset([]);else{let s=GT(e,i);n.reset(s,fE),n.notifyOnChanges()}return!0}return!1}function im(){return UT(Ye(),Ah())}function al(n,e){return n<<17|e<<2}function ps(n){return n>>17&32767}function oC(n){return(n&2)==2}function aC(n,e){return n&131071|e<<17}function Cp(n){return n|2}function io(n){return(n&131068)>>2}function Wh(n,e){return n&-131069|e<<2}function cC(n){return(n&1)===1}function wp(n){return n|1}function lC(n,e,t,i,r,s){let o=s?e.classBindings:e.styleBindings,a=ps(o),c=io(o);n[i]=t;let l=!1,u;if(Array.isArray(t)){let f=t;u=f[1],(u===null||Ws(f,u)>0)&&(l=!0)}else u=t;if(r)if(c!==0){let d=ps(n[a+1]);n[i+1]=al(d,a),d!==0&&(n[d+1]=Wh(n[d+1],i)),n[a+1]=aC(n[a+1],i)}else n[i+1]=al(a,0),a!==0&&(n[a+1]=Wh(n[a+1],i)),a=i;else n[i+1]=al(c,0),a===0?a=i:n[c+1]=Wh(n[c+1],i),c=i;l&&(n[i+1]=Cp(n[i+1])),Wy(n,u,i,!0),Wy(n,u,i,!1),uC(e,u,n,i,s),o=al(a,c),s?e.classBindings=o:e.styleBindings=o}function uC(n,e,t,i,r){let s=r?n.residualClasses:n.residualStyles;s!=null&&typeof e=="string"&&Ws(s,e)>=0&&(t[i+1]=wp(t[i+1]))}function Wy(n,e,t,i){let r=n[t+1],s=e===null,o=i?ps(r):io(r),a=!1;for(;o!==0&&(a===!1||s);){let c=n[o],l=n[o+1];dC(c,e)&&(a=!0,n[o+1]=i?wp(l):Cp(l)),o=i?ps(l):io(l)}a&&(n[t+1]=i?Cp(r):wp(r))}function dC(n,e){return n===null||e==null||(Array.isArray(n)?n[1]:n)===e?!0:Array.isArray(n)&&typeof e=="string"?Ws(n,e)>=0:!1}function Sr(n,e){return fC(n,e,null,!0),Sr}function fC(n,e,t,i){let r=Ye(),s=nn(),o=Ih(2);if(s.firstUpdatePass&&pC(s,n,o,i),e!==qn&&bi(r,o,e)){let a=s.data[Wi()];_C(s,a,r,r[wt],n,r[o+1]=xC(e,t),i,o)}}function hC(n,e){return e>=n.expandoStartIndex}function pC(n,e,t,i){let r=n.data;if(r[t+1]===null){let s=r[Wi()],o=hC(n,t);MC(s,i)&&e===null&&!o&&(e=!1),e=mC(r,s,e,i),lC(r,s,e,t,o,i)}}function mC(n,e,t,i){let r=Qv(n),s=i?e.residualClasses:e.residualStyles;if(r===null)(i?e.classBindings:e.styleBindings)===0&&(t=jh(null,n,e,t,i),t=pa(t,e.attrs,i),s=null);else{let o=e.directiveStylingLast;if(o===-1||n[o]!==r)if(t=jh(r,n,e,t,i),s===null){let c=gC(n,e,i);c!==void 0&&Array.isArray(c)&&(c=jh(null,n,e,c[1],i),c=pa(c,e.attrs,i),vC(n,e,i,c))}else s=yC(n,e,i)}return s!==void 0&&(i?e.residualClasses=s:e.residualStyles=s),t}function gC(n,e,t){let i=t?e.classBindings:e.styleBindings;if(io(i)!==0)return n[ps(i)]}function vC(n,e,t,i){let r=t?e.classBindings:e.styleBindings;n[ps(r)]=i}function yC(n,e,t){let i,r=e.directiveEnd;for(let s=1+e.directiveStylingLast;s<r;s++){let o=n[s].hostAttrs;i=pa(i,o,t)}return pa(i,e.attrs,t)}function jh(n,e,t,i,r){let s=null,o=t.directiveEnd,a=t.directiveStylingLast;for(a===-1?a=t.directiveStart:a++;a<o&&(s=e[a],i=pa(i,s.hostAttrs,r),s!==n);)a++;return n!==null&&(t.directiveStylingLast=a),i}function pa(n,e,t){let i=t?1:2,r=-1;if(e!==null)for(let s=0;s<e.length;s++){let o=e[s];typeof o=="number"?r=o:r===i&&(Array.isArray(n)||(n=n===void 0?[]:["",n]),Cv(n,o,t?!0:e[++s]))}return n===void 0?null:n}function _C(n,e,t,i,r,s,o,a){if(!(e.type&3))return;let c=n.data,l=c[a+1],u=cC(l)?jy(c,e,t,r,io(l),o):void 0;if(!Tl(u)){Tl(s)||oC(l)&&(s=jy(c,null,t,r,a,o));let f=mh(Wi(),t);MS(i,o,f,r,s)}}function jy(n,e,t,i,r,s){let o=e===null,a;for(;r>0;){let c=n[r],l=Array.isArray(c),u=l?c[1]:c,f=u===null,d=t[r+1];d===qn&&(d=f?Bn:void 0);let h=f?zc(d,i):u===i?d:void 0;if(l&&!Tl(h)&&(h=zc(c,i)),Tl(h)&&(a=h,o))return a;let g=n[r+1];r=o?ps(g):io(g)}if(e!==null){let c=s?e.residualClasses:e.residualStyles;c!=null&&(a=zc(c,i))}return a}function Tl(n){return n!==void 0}function xC(n,e){return n==null||n===""||(typeof e=="string"?n=n+e:typeof n=="object"&&(n=Oc(Ei(n)))),n}function MC(n,e){return(n.flags&(e?8:16))!==0}function ut(n,e=""){let t=Ye(),i=nn(),r=n+Ht,s=i.firstCreatePass?ya(i,r,1,e,null):i.data[r],o=bC(i,t,s,e);t[r]=o,el()&&Wp(i,t,o,s),Zs(s,!1)}var bC=(n,e,t,i)=>(tl(!0),BE(e[wt],i));function EC(n,e,t,i=""){return bi(n,ls(),t)?e+ts(t)+i:qn}function SC(n,e,t,i,r,s=""){let o=Xv(),a=vT(n,o,t,r);return Ih(2),a?e+ts(t)+i+ts(r)+s:qn}function un(n){return Ea("",n),un}function Ea(n,e,t){let i=Ye(),r=EC(i,n,e,t);return r!==qn&&w0(i,Wi(),r),Ea}function $l(n,e,t,i,r){let s=Ye(),o=SC(s,n,e,t,i,r);return o!==qn&&w0(s,Wi(),o),$l}function w0(n,e,t){let i=mh(e,n);VE(n[wt],i,t)}function TC(n,e){let t=n[e];return t===qn?void 0:t}function CC(n,e,t,i,r,s){let o=e+t;return bi(n,o,r)?gT(n,o+1,s?i.call(s,r):i(r)):TC(n,o+1)}function nt(n,e){let t=nn(),i,r=n+Ht;t.firstCreatePass?(i=wC(e,t.pipeRegistry),t.data[r]=i,i.onDestroy&&(t.destroyHooks??=[]).push(r,i.onDestroy)):i=t.data[r];let s=i.factory||(i.factory=Zr(i.type,!0)),o,a=_n(ao);try{let c=gl(!1),l=s();return gl(c),gh(t,Ye(),r,l),l}finally{_n(a)}}function wC(n,e){if(e)for(let t=e.length-1;t>=0;t--){let i=e[t];if(n===i.name)return i}}function it(n,e,t){let i=n+Ht,r=Ye(),s=Ov(r,i);return DC(r,i)?CC(r,qv(),e,s.transform,t,s):s.transform(t)}function DC(n,e){return n[we].data[e].pure}var D0=(()=>{class n{applicationErrorHandler=pe(Mr);appRef=pe(Ma);taskService=pe(Ks);ngZone=pe(Hn);zonelessEnabled=pe(ia);tracing=pe(so,{optional:!0});zoneIsDefined=typeof Zone<"u"&&!!Zone.root.run;schedulerTickApplyArgs=[{data:{__scheduler_tick__:!0}}];subscriptions=new yn;angularZoneId=this.zoneIsDefined?this.ngZone._inner?.get(qo):null;scheduleInRootZone=!this.zonelessEnabled&&this.zoneIsDefined&&(pe(kh,{optional:!0})??!1);cancelScheduledCallback=null;useMicrotaskScheduler=!1;runningTick=!1;pendingRenderTaskId=null;constructor(){this.subscriptions.add(this.appRef.afterTick.subscribe(()=>{let t=this.taskService.add();if(!this.runningTick&&(this.cleanup(),!this.zonelessEnabled||this.appRef.includeAllTestViews)){this.taskService.remove(t);return}this.switchToMicrotaskScheduler(),this.taskService.remove(t)})),this.subscriptions.add(this.ngZone.onUnstable.subscribe(()=>{this.runningTick||this.cleanup()}))}switchToMicrotaskScheduler(){this.ngZone.runOutsideAngular(()=>{let t=this.taskService.add();this.useMicrotaskScheduler=!0,queueMicrotask(()=>{this.useMicrotaskScheduler=!1,this.taskService.remove(t)})})}notify(t){if(!this.zonelessEnabled&&t===5)return;switch(t){case 0:{this.appRef.dirtyFlags|=2;break}case 3:case 2:case 4:case 5:case 1:{this.appRef.dirtyFlags|=4;break}case 6:{this.appRef.dirtyFlags|=2;break}case 12:{this.appRef.dirtyFlags|=16;break}case 13:{this.appRef.dirtyFlags|=2;break}case 11:break;default:this.appRef.dirtyFlags|=8}if(this.appRef.tracingSnapshot=this.tracing?.snapshot(this.appRef.tracingSnapshot)??null,!this.shouldScheduleTick())return;let i=this.useMicrotaskScheduler?ly:Ph;this.pendingRenderTaskId=this.taskService.add(),this.scheduleInRootZone?this.cancelScheduledCallback=Zone.root.run(()=>i(()=>this.tick())):this.cancelScheduledCallback=this.ngZone.runOutsideAngular(()=>i(()=>this.tick()))}shouldScheduleTick(){return!(this.appRef.destroyed||this.pendingRenderTaskId!==null||this.runningTick||this.appRef._runningTick||!this.zonelessEnabled&&this.zoneIsDefined&&Zone.current.get(qo+this.angularZoneId))}tick(){if(this.runningTick||this.appRef.destroyed)return;if(this.appRef.dirtyFlags===0){this.cleanup();return}!this.zonelessEnabled&&this.appRef.dirtyFlags&7&&(this.appRef.dirtyFlags|=1);let t=this.taskService.add();try{this.ngZone.run(()=>{this.runningTick=!0,this.appRef._tick()},void 0,this.schedulerTickApplyArgs)}catch(i){this.applicationErrorHandler(i)}finally{this.taskService.remove(t),this.cleanup()}}ngOnDestroy(){this.subscriptions.unsubscribe(),this.cleanup()}cleanup(){if(this.runningTick=!1,this.cancelScheduledCallback?.(),this.cancelScheduledCallback=null,this.pendingRenderTaskId!==null){let t=this.pendingRenderTaskId;this.pendingRenderTaskId=null,this.taskService.remove(t)}}static \u0275fac=function(i){return new(i||n)};static \u0275prov=at({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();function I0(){return[{provide:Kr,useExisting:D0},{provide:Hn,useClass:Xo},{provide:ia,useValue:!0}]}function IC(){return typeof $localize<"u"&&$localize.locale||ba}var ql=new Qe("",{factory:()=>pe(ql,{optional:!0,skipSelf:!0})||IC()});function Tr(n,e){return xc(n,e?.equal)}var R0=Symbol("InputSignalNode#UNSET"),qC=Wt(It({},Mc),{transformFn:void 0,applyValueToInputSignal(n,e){Ho(n,e)}});function N0(n,e){let t=Object.create(qC);t.value=n,t.transformFn=e?.transform;function i(){if(Uo(t),t.value===R0){let r=null;throw new He(-950,r)}return t.value}return i[Mn]=t,i}function A0(n,e){return N0(n,e)}function XC(n){return N0(R0,n)}var Ta=(A0.required=XC,A0);var rm=new Qe(""),YC=new Qe("");function Sa(n){return!n.moduleRef}function ZC(n){let e=Sa(n)?n.r3Injector:n.moduleRef.injector,t=e.get(Hn);return t.run(()=>{Sa(n)?n.r3Injector.resolveInjectorInitializers():n.moduleRef.resolveInjectorInitializers();let i=e.get(Mr),r;if(t.runOutsideAngular(()=>{r=t.onError.subscribe({next:i})}),Sa(n)){let s=()=>e.destroy(),o=n.platformInjector.get(rm);o.add(s),e.onDestroy(()=>{r.unsubscribe(),o.delete(s)})}else{let s=()=>n.moduleRef.destroy(),o=n.platformInjector.get(rm);o.add(s),n.moduleRef.onDestroy(()=>{oa(n.allPlatformModules,n.moduleRef),r.unsubscribe(),o.delete(s)})}return KC(i,t,()=>{let s=e.get(Ks),o=s.add(),a=e.get(em);return a.runInitializers(),a.donePromise.then(()=>{let c=e.get(ql,ba);if(C0(c||ba),!e.get(YC,!0))return Sa(n)?e.get(Ma):(n.allPlatformModules.push(n.moduleRef),n.moduleRef);if(Sa(n)){let u=e.get(Ma);return n.rootComponent!==void 0&&u.bootstrap(n.rootComponent),u}else return JC?.(n.moduleRef,n.allPlatformModules),n.moduleRef}).finally(()=>{s.remove(o)})})})}var JC;function KC(n,e,t){try{let i=t();return Wl(i)?i.catch(r=>{throw e.runOutsideAngular(()=>n(r)),r}):i}catch(i){throw e.runOutsideAngular(()=>n(i)),i}}var Xl=null;function QC(n=[],e){return ei.create({name:e,providers:[{provide:Ko,useValue:"platform"},{provide:rm,useValue:new Set([()=>Xl=null])},...n]})}function ew(n=[]){if(Xl)return Xl;let e=QC(n);return Xl=e,E0(),tw(e),e}function tw(n){let e=n.get(Il,null);Gc(n,()=>{e?.forEach(t=>t())})}var nw=1e4;var VB=nw-1e3;function P0(n){let{rootComponent:e,appProviders:t,platformProviders:i,platformRef:r}=n;yt(ct.BootstrapApplicationStart);try{let s=r?.injector??ew(i),o=[I0(),dy,...t||[]],a=new ha({providers:o,parent:s,debugName:"",runEnvironmentInitializers:!1});return ZC({r3Injector:a.injector,platformInjector:s,rootComponent:e})}catch(s){return Promise.reject(s)}finally{yt(ct.BootstrapApplicationEnd)}}var L0=null;function ms(){return L0}function sm(n){L0??=n}var Ca=class{};var om=(()=>{class n{_viewContainer;_context=new Yl;_thenTemplateRef=null;_elseTemplateRef=null;_thenViewRef=null;_elseViewRef=null;constructor(t,i){this._viewContainer=t,this._thenTemplateRef=i}set ngIf(t){this._context.$implicit=this._context.ngIf=t,this._updateView()}set ngIfThen(t){F0(t,!1),this._thenTemplateRef=t,this._thenViewRef=null,this._updateView()}set ngIfElse(t){F0(t,!1),this._elseTemplateRef=t,this._elseViewRef=null,this._updateView()}_updateView(){this._context.$implicit?this._thenViewRef||(this._viewContainer.clear(),this._elseViewRef=null,this._thenTemplateRef&&(this._thenViewRef=this._viewContainer.createEmbeddedView(this._thenTemplateRef,this._context))):this._elseViewRef||(this._viewContainer.clear(),this._thenViewRef=null,this._elseTemplateRef&&(this._elseViewRef=this._viewContainer.createEmbeddedView(this._elseTemplateRef,this._context)))}static ngIfUseIfTypeGuard;static ngTemplateGuard_ngIf;static ngTemplateContextGuard(t,i){return!0}static \u0275fac=function(i){return new(i||n)(ao(co),ao(fs))};static \u0275dir=xa({type:n,selectors:[["","ngIf",""]],inputs:{ngIf:"ngIf",ngIfThen:"ngIfThen",ngIfElse:"ngIfElse"}})}return n})(),Yl=class{$implicit=null;ngIf=null};function F0(n,e){if(n&&!n.createEmbeddedView)throw new He(2020,!1)}var Zl=(()=>{class n{static \u0275fac=function(i){return new(i||n)};static \u0275mod=_a({type:n});static \u0275inj=Gs({})}return n})();function am(n,e){e=encodeURIComponent(e);for(let t of n.split(";")){let i=t.indexOf("="),[r,s]=i==-1?[t,""]:[t.slice(0,i),t.slice(i+1)];if(r.trim()===e)return decodeURIComponent(s)}return null}var wa=class{};var O0="browser";var Da=class{_doc;constructor(e){this._doc=e}manager},Jl=(()=>{class n extends Da{constructor(t){super(t)}supports(t){return!0}addEventListener(t,i,r,s){return t.addEventListener(i,r,s),()=>this.removeEventListener(t,i,r,s)}removeEventListener(t,i,r,s){return t.removeEventListener(i,r,s)}static \u0275fac=function(i){return new(i||n)(et(cn))};static \u0275prov=at({token:n,factory:n.\u0275fac})}return n})(),eu=new Qe(""),dm=(()=>{class n{_zone;_plugins;_eventNameToPlugin=new Map;constructor(t,i){this._zone=i,t.forEach(o=>{o.manager=this});let r=t.filter(o=>!(o instanceof Jl));this._plugins=r.slice().reverse();let s=t.find(o=>o instanceof Jl);s&&this._plugins.push(s)}addEventListener(t,i,r,s){return this._findPluginFor(i).addEventListener(t,i,r,s)}getZone(){return this._zone}_findPluginFor(t){let i=this._eventNameToPlugin.get(t);if(i)return i;if(i=this._plugins.find(s=>s.supports(t)),!i)throw new He(5101,!1);return this._eventNameToPlugin.set(t,i),i}static \u0275fac=function(i){return new(i||n)(et(eu),et(Hn))};static \u0275prov=at({token:n,factory:n.\u0275fac})}return n})(),cm="ng-app-id";function k0(n){for(let e of n)e.remove()}function U0(n,e){let t=e.createElement("style");return t.textContent=n,t}function sw(n,e,t,i){let r=n.head?.querySelectorAll(`style[${cm}="${e}"],link[${cm}="${e}"]`);if(r)for(let s of r)s.removeAttribute(cm),s instanceof HTMLLinkElement?i.set(s.href.slice(s.href.lastIndexOf("/")+1),{usage:0,elements:[s]}):s.textContent&&t.set(s.textContent,{usage:0,elements:[s]})}function um(n,e){let t=e.createElement("link");return t.setAttribute("rel","stylesheet"),t.setAttribute("href",n),t}var fm=(()=>{class n{doc;appId;nonce;inline=new Map;external=new Map;hosts=new Set;constructor(t,i,r,s={}){this.doc=t,this.appId=i,this.nonce=r,sw(t,i,this.inline,this.external),this.hosts.add(t.head)}addStyles(t,i){for(let r of t)this.addUsage(r,this.inline,U0);i?.forEach(r=>this.addUsage(r,this.external,um))}removeStyles(t,i){for(let r of t)this.removeUsage(r,this.inline);i?.forEach(r=>this.removeUsage(r,this.external))}addUsage(t,i,r){let s=i.get(t);s?s.usage++:i.set(t,{usage:1,elements:[...this.hosts].map(o=>this.addElement(o,r(t,this.doc)))})}removeUsage(t,i){let r=i.get(t);r&&(r.usage--,r.usage<=0&&(k0(r.elements),i.delete(t)))}ngOnDestroy(){for(let[,{elements:t}]of[...this.inline,...this.external])k0(t);this.hosts.clear()}addHost(t){this.hosts.add(t);for(let[i,{elements:r}]of this.inline)r.push(this.addElement(t,U0(i,this.doc)));for(let[i,{elements:r}]of this.external)r.push(this.addElement(t,um(i,this.doc)))}removeHost(t){this.hosts.delete(t)}addElement(t,i){return this.nonce&&i.setAttribute("nonce",this.nonce),t.appendChild(i)}static \u0275fac=function(i){return new(i||n)(et(cn),et(Dl),et(Al,8),et(ma))};static \u0275prov=at({token:n,factory:n.\u0275fac})}return n})(),lm={svg:"http://www.w3.org/2000/svg",xhtml:"http://www.w3.org/1999/xhtml",xlink:"http://www.w3.org/1999/xlink",xml:"http://www.w3.org/XML/1998/namespace",xmlns:"http://www.w3.org/2000/xmlns/",math:"http://www.w3.org/1998/Math/MathML"},hm=/%COMP%/g;var V0="%COMP%",ow=`_nghost-${V0}`,aw=`_ngcontent-${V0}`,cw=!0,lw=new Qe("",{factory:()=>cw});function uw(n){return aw.replace(hm,n)}function dw(n){return ow.replace(hm,n)}function H0(n,e){return e.map(t=>t.replace(hm,n))}var pm=(()=>{class n{eventManager;sharedStylesHost;appId;removeStylesOnCompDestroy;doc;ngZone;nonce;tracingService;rendererByCompId=new Map;defaultRenderer;constructor(t,i,r,s,o,a,c=null,l=null){this.eventManager=t,this.sharedStylesHost=i,this.appId=r,this.removeStylesOnCompDestroy=s,this.doc=o,this.ngZone=a,this.nonce=c,this.tracingService=l,this.defaultRenderer=new Ia(t,o,a,this.tracingService)}createRenderer(t,i){if(!t||!i)return this.defaultRenderer;let r=this.getOrCreateRenderer(t,i);return r instanceof Ql?r.applyToHost(t):r instanceof Aa&&r.applyStyles(),r}getOrCreateRenderer(t,i){let r=this.rendererByCompId,s=r.get(i.id);if(!s){let o=this.doc,a=this.ngZone,c=this.eventManager,l=this.sharedStylesHost,u=this.removeStylesOnCompDestroy,f=this.tracingService;switch(i.encapsulation){case ai.Emulated:s=new Ql(c,l,i,this.appId,u,o,a,f);break;case ai.ShadowDom:return new Kl(c,t,i,o,a,this.nonce,f,l);case ai.ExperimentalIsolatedShadowDom:return new Kl(c,t,i,o,a,this.nonce,f);default:s=new Aa(c,l,i,u,o,a,f);break}r.set(i.id,s)}return s}ngOnDestroy(){this.rendererByCompId.clear()}componentReplaced(t){this.rendererByCompId.delete(t)}static \u0275fac=function(i){return new(i||n)(et(dm),et(fm),et(Dl),et(lw),et(cn),et(Hn),et(Al),et(so,8))};static \u0275prov=at({token:n,factory:n.\u0275fac})}return n})(),Ia=class{eventManager;doc;ngZone;tracingService;data=Object.create(null);throwOnSyntheticProps=!0;constructor(e,t,i,r){this.eventManager=e,this.doc=t,this.ngZone=i,this.tracingService=r}destroy(){}destroyNode=null;createElement(e,t){return t?this.doc.createElementNS(lm[t]||t,e):this.doc.createElement(e)}createComment(e){return this.doc.createComment(e)}createText(e){return this.doc.createTextNode(e)}appendChild(e,t){(B0(e)?e.content:e).appendChild(t)}insertBefore(e,t,i){e&&(B0(e)?e.content:e).insertBefore(t,i)}removeChild(e,t){t.remove()}selectRootElement(e,t){let i=typeof e=="string"?this.doc.querySelector(e):e;if(!i)throw new He(-5104,!1);return t||(i.textContent=""),i}parentNode(e){return e.parentNode}nextSibling(e){return e.nextSibling}setAttribute(e,t,i,r){if(r){t=r+":"+t;let s=lm[r];s?e.setAttributeNS(s,t,i):e.setAttribute(t,i)}else e.setAttribute(t,i)}removeAttribute(e,t,i){if(i){let r=lm[i];r?e.removeAttributeNS(r,t):e.removeAttribute(`${i}:${t}`)}else e.removeAttribute(t)}addClass(e,t){e.classList.add(t)}removeClass(e,t){e.classList.remove(t)}setStyle(e,t,i,r){r&(Mi.DashCase|Mi.Important)?e.style.setProperty(t,i,r&Mi.Important?"important":""):e.style[t]=i}removeStyle(e,t,i){i&Mi.DashCase?e.style.removeProperty(t):e.style[t]=""}setProperty(e,t,i){e!=null&&(e[t]=i)}setValue(e,t){e.nodeValue=t}listen(e,t,i,r){if(typeof e=="string"&&(e=ms().getGlobalEventTarget(this.doc,e),!e))throw new He(5102,!1);let s=this.decoratePreventDefault(i);return this.tracingService?.wrapEventListener&&(s=this.tracingService.wrapEventListener(e,t,s)),this.eventManager.addEventListener(e,t,s,r)}decoratePreventDefault(e){return t=>{if(t==="__ngUnwrap__")return e;e(t)===!1&&t.preventDefault()}}};function B0(n){return n.tagName==="TEMPLATE"&&n.content!==void 0}var Kl=class extends Ia{hostEl;sharedStylesHost;shadowRoot;constructor(e,t,i,r,s,o,a,c){super(e,r,s,a),this.hostEl=t,this.sharedStylesHost=c,this.shadowRoot=t.attachShadow({mode:"open"}),this.sharedStylesHost&&this.sharedStylesHost.addHost(this.shadowRoot);let l=i.styles;l=H0(i.id,l);for(let f of l){let d=document.createElement("style");o&&d.setAttribute("nonce",o),d.textContent=f,this.shadowRoot.appendChild(d)}let u=i.getExternalStyles?.();if(u)for(let f of u){let d=um(f,r);o&&d.setAttribute("nonce",o),this.shadowRoot.appendChild(d)}}nodeOrShadowRoot(e){return e===this.hostEl?this.shadowRoot:e}appendChild(e,t){return super.appendChild(this.nodeOrShadowRoot(e),t)}insertBefore(e,t,i){return super.insertBefore(this.nodeOrShadowRoot(e),t,i)}removeChild(e,t){return super.removeChild(null,t)}parentNode(e){return this.nodeOrShadowRoot(super.parentNode(this.nodeOrShadowRoot(e)))}destroy(){this.sharedStylesHost&&this.sharedStylesHost.removeHost(this.shadowRoot)}},Aa=class extends Ia{sharedStylesHost;removeStylesOnCompDestroy;styles;styleUrls;constructor(e,t,i,r,s,o,a,c){super(e,s,o,a),this.sharedStylesHost=t,this.removeStylesOnCompDestroy=r;let l=i.styles;this.styles=c?H0(c,l):l,this.styleUrls=i.getExternalStyles?.(c)}applyStyles(){this.sharedStylesHost.addStyles(this.styles,this.styleUrls)}destroy(){this.removeStylesOnCompDestroy&&ds.size===0&&this.sharedStylesHost.removeStyles(this.styles,this.styleUrls)}},Ql=class extends Aa{contentAttr;hostAttr;constructor(e,t,i,r,s,o,a,c){let l=r+"-"+i.id;super(e,t,i,s,o,a,c,l),this.contentAttr=uw(l),this.hostAttr=dw(l)}applyToHost(e){this.applyStyles(),this.setAttribute(e,this.hostAttr,"")}createElement(e,t){let i=super.createElement(e,t);return super.setAttribute(i,this.contentAttr,""),i}};var tu=class n extends Ca{supportsDOMEvents=!0;static makeCurrent(){sm(new n)}onAndCancel(e,t,i,r){return e.addEventListener(t,i,r),()=>{e.removeEventListener(t,i,r)}}dispatchEvent(e,t){e.dispatchEvent(t)}remove(e){e.remove()}createElement(e,t){return t=t||this.getDefaultDocument(),t.createElement(e)}createHtmlDocument(){return document.implementation.createHTMLDocument("fakeTitle")}getDefaultDocument(){return document}isElementNode(e){return e.nodeType===Node.ELEMENT_NODE}isShadowRoot(e){return e instanceof DocumentFragment}getGlobalEventTarget(e,t){return t==="window"?window:t==="document"?e:t==="body"?e.body:null}getBaseHref(e){let t=fw();return t==null?null:hw(t)}resetBaseElement(){Ra=null}getUserAgent(){return window.navigator.userAgent}getCookie(e){return am(document.cookie,e)}},Ra=null;function fw(){return Ra=Ra||document.head.querySelector("base"),Ra?Ra.getAttribute("href"):null}function hw(n){return new URL(n,document.baseURI).pathname}var pw=(()=>{class n{build(){return new XMLHttpRequest}static \u0275fac=function(i){return new(i||n)};static \u0275prov=at({token:n,factory:n.\u0275fac})}return n})(),z0=["alt","control","meta","shift"],mw={"\b":"Backspace","	":"Tab","\x7F":"Delete","\x1B":"Escape",Del:"Delete",Esc:"Escape",Left:"ArrowLeft",Right:"ArrowRight",Up:"ArrowUp",Down:"ArrowDown",Menu:"ContextMenu",Scroll:"ScrollLock",Win:"OS"},gw={alt:n=>n.altKey,control:n=>n.ctrlKey,meta:n=>n.metaKey,shift:n=>n.shiftKey},G0=(()=>{class n extends Da{constructor(t){super(t)}supports(t){return n.parseEventName(t)!=null}addEventListener(t,i,r,s){let o=n.parseEventName(i),a=n.eventCallback(o.fullKey,r,this.manager.getZone());return this.manager.getZone().runOutsideAngular(()=>ms().onAndCancel(t,o.domEventName,a,s))}static parseEventName(t){let i=t.toLowerCase().split("."),r=i.shift();if(i.length===0||!(r==="keydown"||r==="keyup"))return null;let s=n._normalizeKey(i.pop()),o="",a=i.indexOf("code");if(a>-1&&(i.splice(a,1),o="code."),z0.forEach(l=>{let u=i.indexOf(l);u>-1&&(i.splice(u,1),o+=l+".")}),o+=s,i.length!=0||s.length===0)return null;let c={};return c.domEventName=r,c.fullKey=o,c}static matchEventFullKeyCode(t,i){let r=mw[t.key]||t.key,s="";return i.indexOf("code.")>-1&&(r=t.code,s="code."),r==null||!r?!1:(r=r.toLowerCase(),r===" "?r="space":r==="."&&(r="dot"),z0.forEach(o=>{if(o!==r){let a=gw[o];a(t)&&(s+=o+".")}}),s+=r,s===i)}static eventCallback(t,i,r){return s=>{n.matchEventFullKeyCode(s,t)&&r.runGuarded(()=>i(s))}}static _normalizeKey(t){return t==="esc"?"escape":t}static \u0275fac=function(i){return new(i||n)(et(cn))};static \u0275prov=at({token:n,factory:n.\u0275fac})}return n})();async function mm(n,e,t){let i=It({rootComponent:n},vw(e,t));return P0(i)}function vw(n,e){return{platformRef:e?.platformRef,appProviders:[...bw,...n?.providers??[]],platformProviders:Mw}}function yw(){tu.makeCurrent()}function _w(){return new Vi}function xw(){return Ap(document),document}var Mw=[{provide:ma,useValue:O0},{provide:Il,useValue:yw,multi:!0},{provide:cn,useFactory:xw}];var bw=[{provide:Ko,useValue:"root"},{provide:Vi,useFactory:_w},{provide:eu,useClass:Jl,multi:!0},{provide:eu,useClass:G0,multi:!0},pm,fm,dm,{provide:hs,useExisting:pm},{provide:wa,useClass:pw},[]];var W0=(()=>{class n{_doc;_dom;constructor(t){this._doc=t,this._dom=ms()}addTag(t,i=!1){return t?this._getOrCreateElement(t,i):null}addTags(t,i=!1){return t?t.reduce((r,s)=>(s&&r.push(this._getOrCreateElement(s,i)),r),[]):[]}getTag(t){return t&&this._doc.querySelector(`meta[${t}]`)||null}getTags(t){if(!t)return[];let i=this._doc.querySelectorAll(`meta[${t}]`);return i?[].slice.call(i):[]}updateTag(t,i){if(!t)return null;i=i||this._parseSelector(t);let r=this.getTag(i);return r?this._setMetaElementAttributes(t,r):this._getOrCreateElement(t,!0)}removeTag(t){this.removeTagElement(this.getTag(t))}removeTagElement(t){t&&this._dom.remove(t)}_getOrCreateElement(t,i=!1){if(!i){let o=this._parseSelector(t),a=this.getTags(o).filter(c=>this._containsAttributes(t,c))[0];if(a!==void 0)return a}let r=this._dom.createElement("meta");return this._setMetaElementAttributes(t,r),this._doc.getElementsByTagName("head")[0].appendChild(r),r}_setMetaElementAttributes(t,i){return Object.keys(t).forEach(r=>i.setAttribute(this._getMetaKeyMap(r),t[r])),i}_parseSelector(t){let i=t.name?"name":"property";return`${i}="${t[i]}"`}_containsAttributes(t,i){return Object.keys(t).every(r=>i.getAttribute(this._getMetaKeyMap(r))===t[r])}_getMetaKeyMap(t){return Ew[t]||t}static \u0275fac=function(i){return new(i||n)(et(cn))};static \u0275prov=at({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})(),Ew={httpEquiv:"http-equiv"},j0=(()=>{class n{_doc;constructor(t){this._doc=t}getTitle(){return this._doc.title}setTitle(t){this._doc.title=t||""}static \u0275fac=function(i){return new(i||n)(et(cn))};static \u0275prov=at({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();var gm=(()=>{class n{static \u0275fac=function(i){return new(i||n)};static \u0275prov=at({token:n,factory:function(i){let r=null;return i?r=new(i||n):r=et(Sw),r},providedIn:"root"})}return n})(),Sw=(()=>{class n extends gm{_doc;constructor(t){super(),this._doc=t}sanitize(t,i){if(i==null)return null;switch(t){case In.NONE:return i;case In.HTML:return $i(i,"HTML")?Ei(i):Nl(this._doc,String(i)).toString();case In.STYLE:return $i(i,"Style")?Ei(i):i;case In.SCRIPT:if($i(i,"Script"))return Ei(i);throw new He(5200,!1);case In.URL:return $i(i,"URL")?Ei(i):ga(String(i));case In.RESOURCE_URL:if($i(i,"ResourceURL"))return Ei(i);throw new He(5201,!1);default:throw new He(5202,!1)}}bypassSecurityTrustHtml(t){return Np(t)}bypassSecurityTrustStyle(t){return Pp(t)}bypassSecurityTrustScript(t){return Lp(t)}bypassSecurityTrustUrl(t){return Fp(t)}bypassSecurityTrustResourceUrl(t){return Op(t)}static \u0275fac=function(i){return new(i||n)(et(cn))};static \u0275prov=at({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();var $0={providers:[Oh()]};var q0={languageLabel:"Language",themeLabel:"Theme",nav:[{id:"hero",label:"Home"},{id:"about",label:"About"},{id:"interests",label:"Interests"},{id:"projects",label:"Projects"},{id:"skills",label:"Skills"},{id:"experience",label:"Experience"},{id:"school",label:"Studies"},{id:"contact",label:"Contact"}],hero:{badge:"Corentin PHILYS",title:"Full Stack Developer focused on application development",subtitle:"I design and build robust, performant and maintainable web, mobile, desktop and native applications.",ctaContact:"Contact me",ctaProjects:"View my projects",ctaCv:"Download my resume"},sectionTitles:{about:"About",interests:"Interests",projects:"Projects",skills:"Skills",experience:"Experience",school:"Studies",contact:"Contact"},labels:{inProgress:"Diploma in progress"},about:{title:"Profile",paragraphs:["As a student of application development, I specialize in designing interactive applications and modern interfaces.","I have gained practical experience through the development of applications for connected interactive kiosks, using Angular for web interfaces and C# for various middleware services, as well as through projects in React Native, Java/Kotlin (Android), and Rust.","I am particularly interested in code quality and creating robust solutions integrated into real-world environments.","I am looking for a work-study program that will allow me to strengthen my web and application development skills within a technical team where I can actively contribute to concrete projects.","Curious, self-motivated, and problem-solving oriented, I bring the ability to adapt quickly, an analytical approach, and a genuine motivation to learn and produce reliable, maintainable solutions."]},interests:["Programming","L0AD (Laboratoire ouvert Dr\xF4me Ard\xE8che)","Nuit de l'Info","Sports (Workout for 4 years)","Motorsports","Board games","Japanese culture"],projects:[{title:"Space Shooter",summary:`Space shooter-style game built using the Godot game engine as part of a university project.
__Objective:__ 3D menu in space with level selection, 2D game with a physics engine. Implementation of various features relevant to a game.
__Achievements:__ Different game modes, integration of a 3D HUD into a 2D game, configuration file for settings, scores, and levels`,stack:["Godot","GdScript","Git"],links:{}},{title:"Aramis",summary:`Mobile app and web-based supervisor interface developed as part of a university project for a company.
__Requirements:__ An app to help order pickers process orders efficiently, along with a web-based administration panel.
__Features:__ Real-time order tracking, 3D pallet visualization, intelligent sorting algorithm, voice recognition.`,stack:["Kotlin","API REST","PostgreSQL","Android SDK","Django","Filament","Vosk","Tests unitaires","Gitlab CI/CD"],links:{}},{title:"Portfolio",summary:"Personal portfolio to showcase my projects and skills.\\n__Objective:__ To present my profile through a showcase website\\__Features:__ Responsive website built with Angular, integration of dynamic content, implementation of a modular architecture and a seamless navigation system.",stack:["Angular","Typescript","HTML/SCSS"],links:{repository:"https://github.com/endercreeps/portfolio/"}}],skills:[{category:"Frontend",items:["Angular","React","TypeScript","JavaScript","HTML","CSS/SCSS"]},{category:"Backend",items:["Node.js","Express","REST API","SQL","PHP"," PL/pgsql","Django","Firebase"]},{category:"Application",items:["Python","C/C++","C#","Kotlin","Java","Rust"]},{category:"Mobile",items:["Flutter","Kotlin","Java","Android SDK","React Native"]},{category:"Environments",items:["VirtualBox","WSL","Gitlab CI/CD","Docker Compose"]},{category:"Operating System",items:["Linux","Windows","Android"]}],experiences:[{role:"[Apprenticeship] Software development for interactive kiosks",company:"IPM France",period:"09/2025 - 08/2026",details:["Teamwork, formalizing client requirements","Optimizing a complex web application (Angular)","Development of a service for proprietary middleware to support a Vital reader (PAX, Vit@Jour) in C#","Refactoring of a service that reads Vitale cards using a PC/SC card reader","Tools: Console POC, Gitlab/Gitea server setup, build pipeline and Jenkins/Gitlab plugins"]},{role:"[Internship] Software development for interactive kiosks",company:"IPM France",period:"03/2025 - 06/2025",details:["Teamwork, defining client requirements","Optimizing a complex web application (Angular) with bugs fixes, new features and Angular8 to Angular19 migration","Creating and optimizing demo web applications (Angular)"]}],school:[{diplome:"[Master's degree] IT (ILSEN)",school:"Universit\xE9 d'Avignon, Avignon, Vaucluse (84)",period:"09/2026 - 07/2028",actual:!0},{diplome:"[Bachelor's degree in science and technology] IT ",school:"IUT de Valence, Valence, Dr\xF4me (26)",period:"09/2023 - 07/2026",actual:!1},{diplome:"[High school diploma] General (Mathematics, Computer Science, Physics and Chemistry) **Good grade**",school:"Lyc\xE9e \xC9douard Branly, Lyon 05, Rh\xF4ne (69)",period:"09/2021 - 07/2023",actual:!1}],contact:{title:"Let us discuss your needs",intro:"I am open to apprenticeship opportunities in full stack application development.",emailLabel:"Email",linkedinLabel:"LinkedIn",githubLabel:"GitHub",locationLabel:"Localisation",email:"",linkedin:"https://www.linkedin.com/in/philysc",github:"https://github.com/endercreeps",location:"Lyon (69) / Avignon (84), France"},footer:{rights:"\xA9 2026 Corentin PHILYS - All rights reserved."}};var X0={languageLabel:"Langue",themeLabel:"Theme",nav:[{id:"hero",label:"Accueil"},{id:"about",label:"\xC0 propos"},{id:"interests",label:"Centres d'int\xE9r\xEAt"},{id:"projects",label:"Projets"},{id:"skills",label:"Comp\xE9tences"},{id:"experience",label:"Exp\xE9riences"},{id:"school",label:"\xC9tudes"},{id:"contact",label:"Contact"}],hero:{badge:"Corentin PHILYS",title:"D\xE9veloppeur Full Stack orient\xE9 d\xE9veloppement applicatif",subtitle:"Je con\xE7ois et impl\xE9mente des applications web, mobile, *desktop* et native.",ctaContact:"Me contacter",ctaProjects:"Voir mes projets",ctaCv:"T\xE9l\xE9charger mon CV"},sectionTitles:{about:"\xC0 propos",interests:"Centres d'int\xE9r\xEAt",projects:"Projets",skills:"Comp\xE9tences",experience:"Exp\xE9rience",school:"\xC9tudes",contact:"Contact"},labels:{inProgress:"Dipl\xF4me en cours"},about:{title:"Profil",paragraphs:["\xC9tudiant en d\xE9veloppement applicatif, je me sp\xE9cialise dans la conception d'applications interactives et d'interfaces modernes.","J'ai acquis une exp\xE9rience concr\xE8te \xE0 travers le d\xE9veloppement d'applications pour bornes interactives connect\xE9es, en utilisant notamment Angular pour les interfaces web ainsi que C# pour les diff\xE9rents services d'un *middleware* mais aussi via des projets en React Native, Java/Kotlin (Android) et Rust.","Je m'int\xE9resse particuli\xE8rement \xE0 la qualit\xE9 du code et \xE0 la cr\xE9ation de solutions robustes int\xE9gr\xE9es dans des environnements r\xE9els.","Je recherche une alternance me permettant de renforcer mes comp\xE9tences en d\xE9veloppement web et applicatif au sein d'une \xE9quipe technique o\xF9 je pourrai contribuer activement \xE0 des projets concrets.","Curieux, autonome et orient\xE9 r\xE9solution de probl\xE8mes, j'apporte une capacit\xE9 d'adaptation rapide, une approche analytique et une r\xE9elle motivation \xE0 apprendre et \xE0 produire des solutions fiables et maintenables."]},interests:["Programmation","L0AD (Laboratoire ouvert Dr\xF4me Ard\xE8che)","Nuit de l'Info","Sport (Musculation depuis 4 ans)","Sport automobile","Jeu de soci\xE9t\xE9","Culture japonaise"],projects:[{title:"Space Shooter",summary:`Jeu type *Space Shooter* avec le moteur de jeu Godot dans le cadre d'un projet universitaire.
__Objectif :__ Menu 3D dans l'espace avec choix de niveaux, jeu en 2D avec moteur physique. Impl\xE9mentation de diff\xE9rentes fonctionnalit\xE9s pertinentes pour un jeu.
__R\xE9alisation :__ Diff\xE9rents modes de jeu, int\xE9gration d'un *HUD* 3D dans un jeu en 2D, fichier de configuration pour les param\xE8tres, les scores et les niveaux`,stack:["Godot","GdScript","Git"],links:{}},{title:"Aramis",summary:`Application mobile et interface superviseur web dans le cadre d'un projet universitaire pour une entreprise.
__Besoin :__ application pour aider les pr\xE9parateurs \xE0 r\xE9aliser les commandes de mani\xE8re efficace et administration sur un panel web.
__R\xE9alisation :__ Suivi des pr\xE9parations en temps r\xE9el, visualisation de palette en 3D, algorithme de tri intelligent, reconnaissance vocal.`,stack:["Kotlin","API REST","PostgreSQL","Android SDK","Django","Filament","Vosk","Tests unitaires","Gitlab CI/CD"],links:{}},{title:"Portfolio",summary:`Portfolio personnel pour pr\xE9senter mes projets et comp\xE9tences.
__Objectif :__ Pr\xE9senter mon profil \xE0 travers un site vitrine
__R\xE9alisation :__ Site web responsive avec Angular, int\xE9gration de contenu dynamique, mise en place d'une architecture modulaire et d'un syst\xE8me de navigation fluide.`,stack:["Angular","Typescript","HTML/SCSS"],links:{repository:"https://github.com/endercreeps/portfolio/"}}],skills:[{category:"Frontend",items:["Angular","React","TypeScript","JavaScript","HTML","CSS/SCSS"]},{category:"Backend",items:["Node.js","Express","REST API","SQL","PHP"," PL/pgsql","Django","Firebase"]},{category:"Applicatif",items:["Python","C/C++","C#","Kotlin","Java","Rust"]},{category:"Mobile",items:["Flutter","Kotlin","Java","Android SDK","React Native"]},{category:"Environnements",items:["VirtualBox","WSL","Gitlab CI/CD","Docker Compose"]},{category:"Syst\xE8mes d'exploitation",items:["Linux","Windows","Android"]}],experiences:[{role:"[Alternance] D\xE9veloppement logiciel de bornes interactives",company:"IPM France",period:"09/2025 - 08/2026",details:["Travail d'\xE9quipe, formalisation du besoin client","Optimisation d'une application web complexe (Angular)","R\xE9alisation d'un service pour middleware propri\xE9taire pour l'utilisation d'un lecteur Vitale (PAX, Vit@Jour) en C#.","*Refactoring* d'un service de lecture de carte Vitale via lecteur PC/SC","Outillage : POC console, mise en place serveur Gitlab/Gitea, chaine de compilation et plugins Jenkins/Gitlab"]},{role:"[Stage] D\xE9veloppement logiciel de bornes interactives",company:"IPM France",period:"03/2025 - 06/2025",details:["Travail d'\xE9quipe, formalisation du besoin client","Optimisation d'une application web complexe (Angular) avec correction de bugs, ajout d'am\xE9lioration et migration Angular8 \xE0 Angular19 ","Cr\xE9ation et optimisations d'applications web de d\xE9monstration (Angular)"]}],school:[{diplome:"[MASTER] Informatique (Parcours ILSEN)",school:"Universit\xE9 d'Avignon, Avignon, Vaucluse (84)",period:"09/2026 - 07/2028",actual:!0},{diplome:"[BUT] Informatique (Parcours r\xE9alisation et d\xE9veloppement d'application)",school:"IUT de Valence, Valence, Dr\xF4me (26)",period:"09/2023 - 07/2026",actual:!1},{diplome:"[BAC] G\xE9n\xE9ral (Math\xE9matiques, Num\xE9rique et Science Informatiques, Physique-Chimie) **Mention Bien**",school:"Lyc\xE9e \xC9douard Branly, Lyon 05, Rh\xF4ne (69)",period:"09/2021 - 07/2023",actual:!1}],contact:{title:"\xC9changeons sur votre besoin",intro:"Je suis ouvert aux opportunit\xE9s d'alternance en d\xE9veloppement applicatif.",emailLabel:"Email",linkedinLabel:"LinkedIn",githubLabel:"GitHub",locationLabel:"Localisation",email:"",linkedin:"https://www.linkedin.com/in/philysc",github:"https://github.com/endercreeps",location:"Lyon (69) / Avignon (84), France"},footer:{rights:"\xA9 2026 Corentin PHILYS - Tous droits r\xE9serv\xE9s."}};var fo={name:"Portfolio",defaultLanguage:"fr",cvPath:"/cv/cv.pdf",seo:{fr:{title:"Portfolio | D\xE9veloppeur  Full Stack",description:"Portfolio one-page professionnel orient\xE9 d\xE9veloppement applicatif full stack, disponible en francais et anglais."},en:{title:"Portfolio | Full Stack Developer",description:"Professional one-page portfolio focused on full stack application development, available in French and English."}}};var Y0="portfolio-language",Ct=class n{currentLanguage=ln(this.resolveInitialLanguage());language=Tr(()=>this.currentLanguage());content=Tr(()=>this.currentLanguage()==="fr"?X0:q0);setLanguage(e){this.currentLanguage.set(e),localStorage.setItem(Y0,e),document.documentElement.lang=e}toggleLanguage(){this.setLanguage(this.currentLanguage()==="fr"?"en":"fr")}resolveInitialLanguage(){let e=localStorage.getItem(Y0),t=e==="fr"||e==="en"?e:fo.defaultLanguage;return document.documentElement.lang=t,t}static \u0275fac=function(t){return new(t||n)};static \u0275prov=at({token:n,factory:n.\u0275fac,providedIn:"root"})};var Z0="portfolio-theme",nu=class n{currentTheme=ln(this.resolveInitialTheme());theme=Tr(()=>this.currentTheme());constructor(){this.applyTheme(this.currentTheme())}toggleTheme(){let e=this.currentTheme()==="dark"?"light":"dark";this.setTheme(e)}setTheme(e){this.currentTheme.set(e),this.applyTheme(e),localStorage.setItem(Z0,e)}applyTheme(e){document.documentElement.setAttribute("data-theme",e)}resolveInitialTheme(){let e=localStorage.getItem(Z0);return e==="light"||e==="dark"?e:typeof window<"u"&&typeof window.matchMedia=="function"&&window.matchMedia("(prefers-color-scheme: dark)").matches?"dark":"light"}static \u0275fac=function(t){return new(t||n)};static \u0275prov=at({token:n,factory:n.\u0275fac,providedIn:"root"})};var Nt=class n{sanitizer=pe(gm);transform(e){if(!e)return"";let i=this.escapeHtml(e).replace(/\*\*(.+?)\*\*/g,"<strong>$1</strong>").replace(/__(.+?)__/g,"<u>$1</u>").replace(/(?<!\*)\*(?!\s)(.+?)(?<!\s)\*(?!\*)/g,"<em>$1</em>").replace(/\n/g,"<br>");return this.sanitizer.sanitize(In.HTML,i)??""}escapeHtml(e){return e.replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;").replace(/"/g,"&quot;").replace(/'/g,"&#39;")}static \u0275fac=function(t){return new(t||n)};static \u0275pipe=zl({name:"richText",type:n,pure:!0})};var Tw=(n,e)=>e.id;function Cw(n,e){if(n&1){let t=lo();Ut(0,"button",14),nt(1,"richText"),Er("click",function(){let r=yr(t).$implicit,s=En();return _r(s.scrollToSection(r.id))}),$t()}if(n&2){let t=e.$implicit;rn("innerHTML",it(1,1,t.label),tt)}}var iu=class n{i18n=pe(Ct);theme=pe(nu);content=this.i18n.content;language=this.i18n.language;currentTheme=this.theme.theme;scrollToSection(e){document.getElementById(e)?.scrollIntoView({behavior:"smooth",block:"start"})}switchLanguage(e){this.i18n.setLanguage(e)}toggleTheme(){this.theme.toggleTheme()}static \u0275fac=function(t){return new(t||n)};static \u0275cmp=Mt({type:n,selectors:[["app-header"]],decls:20,vars:9,consts:[["role","banner",1,"site-header"],[1,"header-inner","container"],["type","button","aria-label","Home section",1,"brand",3,"click"],["src","logo.png","alt","Portfolio",1,"brand-logo"],["aria-label","Primary",1,"nav"],["type","button",1,"nav-link",3,"innerHTML"],[1,"controls"],[1,"language-switch"],["type","button","aria-label","Switch to French",3,"click"],["type","button","aria-label","Switch to English",3,"click"],["type","button",1,"theme-toggle",3,"click"],["aria-hidden","true",1,"theme-glyph"],["aria-hidden","true",1,"theme-switch-track"],[1,"theme-switch-thumb"],["type","button",1,"nav-link",3,"click","innerHTML"]],template:function(t,i){t&1&&(Ut(0,"header",0)(1,"div",1)(2,"button",2),Er("click",function(){return i.scrollToSection("hero")}),zt(3,"img",3),$t(),Ut(4,"nav",4),Jt(5,Cw,2,3,"button",5,Tw),$t(),Ut(7,"div",6)(8,"div",7)(9,"button",8),Er("click",function(){return i.switchLanguage("fr")}),ut(10," FR "),$t(),Ut(11,"button",9),Er("click",function(){return i.switchLanguage("en")}),ut(12," EN "),$t()(),Ut(13,"button",10),Er("click",function(){return i.toggleTheme()}),Ut(14,"span",11),ut(15,"\u2600"),$t(),Ut(16,"span",12),zt(17,"span",13),$t(),Ut(18,"span",11),ut(19,"\u263E"),$t()()()()()),t&2&&(oe(5),Kt(i.content().nav),oe(3),Yi("aria-label",i.content().languageLabel),oe(),Sr("is-active",i.language()==="fr"),oe(2),Sr("is-active",i.language()==="en"),oe(2),Sr("is-dark",i.currentTheme()==="dark"),Yi("aria-label",i.content().themeLabel)("aria-pressed",i.currentTheme()==="dark"))},dependencies:[Nt],styles:[".site-header[_ngcontent-%COMP%]{position:sticky;top:0;z-index:40;background:color-mix(in srgb,var(--surface-primary) 84%,transparent);-webkit-backdrop-filter:blur(12px);backdrop-filter:blur(12px);border-bottom:1px solid var(--border-color)}.header-inner[_ngcontent-%COMP%]{min-height:4.5rem;display:flex;align-items:center;justify-content:space-between;gap:1rem}.brand[_ngcontent-%COMP%]{border:none;background:transparent;padding:.2rem;line-height:0;cursor:pointer}.brand-logo[_ngcontent-%COMP%]{display:block;height:2.5rem;width:auto}[data-theme=dark][_nghost-%COMP%]   .brand-logo[_ngcontent-%COMP%], [data-theme=dark]   [_nghost-%COMP%]   .brand-logo[_ngcontent-%COMP%]{filter:invert(1) brightness(1.35) contrast(1.08)}.nav[_ngcontent-%COMP%]{display:none;gap:.35rem}.nav-link[_ngcontent-%COMP%], .language-switch[_ngcontent-%COMP%]   button[_ngcontent-%COMP%]{border:1px solid transparent;background:transparent;color:var(--text-secondary);border-radius:.5rem;padding:.45rem .65rem;cursor:pointer;transition:color .18s ease,background-color .18s ease,border-color .18s ease}.nav-link[_ngcontent-%COMP%]:hover, .language-switch[_ngcontent-%COMP%]   button[_ngcontent-%COMP%]:hover{color:var(--text-primary);border-color:var(--border-color);background:var(--surface-secondary)}.theme-toggle[_ngcontent-%COMP%]{border:1px solid var(--border-color);background:linear-gradient(90deg,color-mix(in srgb,#ffd857 26%,var(--surface-secondary)),color-mix(in srgb,#1e3a8a 26%,var(--surface-secondary)));color:var(--text-secondary);border-radius:999px;padding:.28rem .45rem;cursor:pointer;display:inline-flex;align-items:center;gap:.35rem;transition:border-color .18s ease,background-color .18s ease}.theme-toggle[_ngcontent-%COMP%]:hover{border-color:color-mix(in srgb,var(--accent) 35%,var(--border-color));background:var(--surface-secondary)}.theme-glyph[_ngcontent-%COMP%]{font-size:.75rem;line-height:1}.theme-glyph[_ngcontent-%COMP%]:first-child{color:#f4b400;text-shadow:0 0 8px rgba(255,216,87,.45)}.theme-glyph[_ngcontent-%COMP%]:last-child{color:#1e3a8a;text-shadow:0 0 8px rgba(30,58,138,.35)}.theme-switch-track[_ngcontent-%COMP%]{position:relative;width:2.25rem;height:1.2rem;border-radius:999px;background:linear-gradient(90deg,color-mix(in srgb,#ffd857 35%,var(--surface-secondary)),color-mix(in srgb,#1e3a8a 40%,var(--surface-secondary)));border:1px solid color-mix(in srgb,#1e3a8a 20%,var(--border-color))}.theme-switch-thumb[_ngcontent-%COMP%]{position:absolute;top:50%;left:.14rem;width:.82rem;height:.82rem;border-radius:50%;background:color-mix(in srgb,var(--surface-primary) 92%,white 8%);transform:translateY(-50%);box-shadow:0 2px 6px #00000047;transition:left .18s ease,background-color .18s ease}.theme-toggle.is-dark[_ngcontent-%COMP%]   .theme-switch-thumb[_ngcontent-%COMP%]{left:1.18rem;background:color-mix(in srgb,var(--accent) 22%,white 78%)}.theme-toggle.is-dark[_ngcontent-%COMP%]   .theme-switch-track[_ngcontent-%COMP%]{background:linear-gradient(90deg,color-mix(in srgb,#ffd857 22%,var(--surface-secondary)),color-mix(in srgb,#1e3a8a 55%,var(--surface-secondary)))}.controls[_ngcontent-%COMP%]{display:flex;align-items:center;gap:.5rem}.language-switch[_ngcontent-%COMP%]{display:inline-flex;gap:.2rem;padding:.22rem;border:1px solid var(--border-color);border-radius:999px;background:color-mix(in srgb,var(--surface-primary) 88%,transparent)}.language-switch[_ngcontent-%COMP%]   button[_ngcontent-%COMP%]{min-width:2.2rem;padding:.22rem .6rem;border-radius:999px;font-size:.76rem;font-weight:700;letter-spacing:.05em;text-transform:uppercase}.language-switch[_ngcontent-%COMP%]   .is-active[_ngcontent-%COMP%]{background:color-mix(in srgb,var(--accent) 20%,var(--surface-primary));color:var(--text-primary);border-color:color-mix(in srgb,var(--accent) 28%,var(--border-color));box-shadow:inset 0 0 0 1px #ffffff2e}@media(min-width:920px){.nav[_ngcontent-%COMP%]{display:inline-flex}}"]})};var gx=0,Ym=1,vx=2;var rc=1,yx=2,Ro=3,ir=0,xn=1,Ai=2,Ri=0,rr=1,Zm=2,Jm=3,Km=4,_x=5;var Pr=100,xx=101,Mx=102,bx=103,Ex=104,Sx=200,Tx=201,Cx=202,wx=203,wu=204,Du=205,Dx=206,Ix=207,Ax=208,Rx=209,Nx=210,Px=211,Lx=212,Fx=213,Ox=214,Iu=0,Au=1,Ru=2,bs=3,Nu=4,Pu=5,Lu=6,Fu=7,Qm=0,kx=1,Ux=2,pi=0,eg=1,tg=2,ng=3,ig=4,rg=5,sg=6,og=7;var Vm=300,Br=301,Cs=302,sd=303,od=304,sc=306,Ou=1e3,Ci=1001,ku=1002,en=1003,Bx=1004;var oc=1005;var sn=1006,ad=1007;var Vr=1008;var Un=1009,ag=1010,cg=1011,No=1012,cd=1013,mi=1014,gi=1015,Ni=1016,ld=1017,ud=1018,Po=1020,lg=35902,ug=35899,dg=1021,fg=1022,Jn=1023,wi=1026,Hr=1027,hg=1028,dd=1029,ws=1030,fd=1031;var hd=1033,ac=33776,cc=33777,lc=33778,uc=33779,pd=35840,md=35841,gd=35842,vd=35843,yd=36196,_d=37492,xd=37496,Md=37488,bd=37489,Ed=37490,Sd=37491,Td=37808,Cd=37809,wd=37810,Dd=37811,Id=37812,Ad=37813,Rd=37814,Nd=37815,Pd=37816,Ld=37817,Fd=37818,Od=37819,kd=37820,Ud=37821,Bd=36492,Vd=36494,Hd=36495,zd=36283,Gd=36284,Wd=36285,jd=36286;var ka=2300,Uu=2301,Cu=2302,Hm=2303,zm=2400,Gm=2401,Wm=2402;var Vx=3200;var Hx=0,zx=1,or="",Pn="srgb",Es="srgb-linear",Ua="linear",dt="srgb";var xs=7680;var jm=519,Gx=512,Wx=513,jx=514,$d=515,$x=516,qx=517,qd=518,Xx=519,$m=35044;var pg="300 es",hi=2e3,Ba=2001;function ww(n){for(let e=n.length-1;e>=0;--e)if(n[e]>=65535)return!0;return!1}function Dw(n){return ArrayBuffer.isView(n)&&!(n instanceof DataView)}function Va(n){return document.createElementNS("http://www.w3.org/1999/xhtml",n)}function Yx(){let n=Va("canvas");return n.style.display="block",n}var J0={},Co=null;function mg(...n){let e="THREE."+n.shift();Co?Co("log",e,...n):console.log(e,...n)}function Zx(n){let e=n[0];if(typeof e=="string"&&e.startsWith("TSL:")){let t=n[1];t&&t.isStackTrace?n[0]+=" "+t.getLocation():n[1]='Stack trace not available. Enable "THREE.Node.captureStackTrace" to capture stack traces.'}return n}function Le(...n){n=Zx(n);let e="THREE."+n.shift();if(Co)Co("warn",e,...n);else{let t=n[0];t&&t.isStackTrace?console.warn(t.getError(e)):console.warn(e,...n)}}function Ne(...n){n=Zx(n);let e="THREE."+n.shift();if(Co)Co("error",e,...n);else{let t=n[0];t&&t.isStackTrace?console.error(t.getError(e)):console.error(e,...n)}}function Ha(...n){let e=n.join(" ");e in J0||(J0[e]=!0,Le(...n))}function Jx(n,e,t){return new Promise(function(i,r){function s(){switch(n.clientWaitSync(e,n.SYNC_FLUSH_COMMANDS_BIT,0)){case n.WAIT_FAILED:r();break;case n.TIMEOUT_EXPIRED:setTimeout(s,t);break;default:i()}}setTimeout(s,t)})}var Kx={[Iu]:Au,[Ru]:Lu,[Nu]:Fu,[bs]:Pu,[Au]:Iu,[Lu]:Ru,[Fu]:Nu,[Pu]:bs},sr=class{addEventListener(e,t){this._listeners===void 0&&(this._listeners={});let i=this._listeners;i[e]===void 0&&(i[e]=[]),i[e].indexOf(t)===-1&&i[e].push(t)}hasEventListener(e,t){let i=this._listeners;return i===void 0?!1:i[e]!==void 0&&i[e].indexOf(t)!==-1}removeEventListener(e,t){let i=this._listeners;if(i===void 0)return;let r=i[e];if(r!==void 0){let s=r.indexOf(t);s!==-1&&r.splice(s,1)}}dispatchEvent(e){let t=this._listeners;if(t===void 0)return;let i=t[e.type];if(i!==void 0){e.target=this;let r=i.slice(0);for(let s=0,o=r.length;s<o;s++)r[s].call(this,e);e.target=null}}},dn=["00","01","02","03","04","05","06","07","08","09","0a","0b","0c","0d","0e","0f","10","11","12","13","14","15","16","17","18","19","1a","1b","1c","1d","1e","1f","20","21","22","23","24","25","26","27","28","29","2a","2b","2c","2d","2e","2f","30","31","32","33","34","35","36","37","38","39","3a","3b","3c","3d","3e","3f","40","41","42","43","44","45","46","47","48","49","4a","4b","4c","4d","4e","4f","50","51","52","53","54","55","56","57","58","59","5a","5b","5c","5d","5e","5f","60","61","62","63","64","65","66","67","68","69","6a","6b","6c","6d","6e","6f","70","71","72","73","74","75","76","77","78","79","7a","7b","7c","7d","7e","7f","80","81","82","83","84","85","86","87","88","89","8a","8b","8c","8d","8e","8f","90","91","92","93","94","95","96","97","98","99","9a","9b","9c","9d","9e","9f","a0","a1","a2","a3","a4","a5","a6","a7","a8","a9","aa","ab","ac","ad","ae","af","b0","b1","b2","b3","b4","b5","b6","b7","b8","b9","ba","bb","bc","bd","be","bf","c0","c1","c2","c3","c4","c5","c6","c7","c8","c9","ca","cb","cc","cd","ce","cf","d0","d1","d2","d3","d4","d5","d6","d7","d8","d9","da","db","dc","dd","de","df","e0","e1","e2","e3","e4","e5","e6","e7","e8","e9","ea","eb","ec","ed","ee","ef","f0","f1","f2","f3","f4","f5","f6","f7","f8","f9","fa","fb","fc","fd","fe","ff"];var vm=Math.PI/180,Bu=180/Math.PI;function dc(){let n=Math.random()*4294967295|0,e=Math.random()*4294967295|0,t=Math.random()*4294967295|0,i=Math.random()*4294967295|0;return(dn[n&255]+dn[n>>8&255]+dn[n>>16&255]+dn[n>>24&255]+"-"+dn[e&255]+dn[e>>8&255]+"-"+dn[e>>16&15|64]+dn[e>>24&255]+"-"+dn[t&63|128]+dn[t>>8&255]+"-"+dn[t>>16&255]+dn[t>>24&255]+dn[i&255]+dn[i>>8&255]+dn[i>>16&255]+dn[i>>24&255]).toLowerCase()}function Ke(n,e,t){return Math.max(e,Math.min(t,n))}function Iw(n,e){return(n%e+e)%e}function ym(n,e,t){return(1-t)*n+t*e}function Na(n,e){switch(e.constructor){case Float32Array:return n;case Uint32Array:return n/4294967295;case Uint16Array:return n/65535;case Uint8Array:return n/255;case Int32Array:return Math.max(n/2147483647,-1);case Int16Array:return Math.max(n/32767,-1);case Int8Array:return Math.max(n/127,-1);default:throw new Error("Invalid component type.")}}function Sn(n,e){switch(e.constructor){case Float32Array:return n;case Uint32Array:return Math.round(n*4294967295);case Uint16Array:return Math.round(n*65535);case Uint8Array:return Math.round(n*255);case Int32Array:return Math.round(n*2147483647);case Int16Array:return Math.round(n*32767);case Int8Array:return Math.round(n*127);default:throw new Error("Invalid component type.")}}var pt=class n{constructor(e=0,t=0){n.prototype.isVector2=!0,this.x=e,this.y=t}get width(){return this.x}set width(e){this.x=e}get height(){return this.y}set height(e){this.y=e}set(e,t){return this.x=e,this.y=t,this}setScalar(e){return this.x=e,this.y=e,this}setX(e){return this.x=e,this}setY(e){return this.y=e,this}setComponent(e,t){switch(e){case 0:this.x=t;break;case 1:this.y=t;break;default:throw new Error("index is out of range: "+e)}return this}getComponent(e){switch(e){case 0:return this.x;case 1:return this.y;default:throw new Error("index is out of range: "+e)}}clone(){return new this.constructor(this.x,this.y)}copy(e){return this.x=e.x,this.y=e.y,this}add(e){return this.x+=e.x,this.y+=e.y,this}addScalar(e){return this.x+=e,this.y+=e,this}addVectors(e,t){return this.x=e.x+t.x,this.y=e.y+t.y,this}addScaledVector(e,t){return this.x+=e.x*t,this.y+=e.y*t,this}sub(e){return this.x-=e.x,this.y-=e.y,this}subScalar(e){return this.x-=e,this.y-=e,this}subVectors(e,t){return this.x=e.x-t.x,this.y=e.y-t.y,this}multiply(e){return this.x*=e.x,this.y*=e.y,this}multiplyScalar(e){return this.x*=e,this.y*=e,this}divide(e){return this.x/=e.x,this.y/=e.y,this}divideScalar(e){return this.multiplyScalar(1/e)}applyMatrix3(e){let t=this.x,i=this.y,r=e.elements;return this.x=r[0]*t+r[3]*i+r[6],this.y=r[1]*t+r[4]*i+r[7],this}min(e){return this.x=Math.min(this.x,e.x),this.y=Math.min(this.y,e.y),this}max(e){return this.x=Math.max(this.x,e.x),this.y=Math.max(this.y,e.y),this}clamp(e,t){return this.x=Ke(this.x,e.x,t.x),this.y=Ke(this.y,e.y,t.y),this}clampScalar(e,t){return this.x=Ke(this.x,e,t),this.y=Ke(this.y,e,t),this}clampLength(e,t){let i=this.length();return this.divideScalar(i||1).multiplyScalar(Ke(i,e,t))}floor(){return this.x=Math.floor(this.x),this.y=Math.floor(this.y),this}ceil(){return this.x=Math.ceil(this.x),this.y=Math.ceil(this.y),this}round(){return this.x=Math.round(this.x),this.y=Math.round(this.y),this}roundToZero(){return this.x=Math.trunc(this.x),this.y=Math.trunc(this.y),this}negate(){return this.x=-this.x,this.y=-this.y,this}dot(e){return this.x*e.x+this.y*e.y}cross(e){return this.x*e.y-this.y*e.x}lengthSq(){return this.x*this.x+this.y*this.y}length(){return Math.sqrt(this.x*this.x+this.y*this.y)}manhattanLength(){return Math.abs(this.x)+Math.abs(this.y)}normalize(){return this.divideScalar(this.length()||1)}angle(){return Math.atan2(-this.y,-this.x)+Math.PI}angleTo(e){let t=Math.sqrt(this.lengthSq()*e.lengthSq());if(t===0)return Math.PI/2;let i=this.dot(e)/t;return Math.acos(Ke(i,-1,1))}distanceTo(e){return Math.sqrt(this.distanceToSquared(e))}distanceToSquared(e){let t=this.x-e.x,i=this.y-e.y;return t*t+i*i}manhattanDistanceTo(e){return Math.abs(this.x-e.x)+Math.abs(this.y-e.y)}setLength(e){return this.normalize().multiplyScalar(e)}lerp(e,t){return this.x+=(e.x-this.x)*t,this.y+=(e.y-this.y)*t,this}lerpVectors(e,t,i){return this.x=e.x+(t.x-e.x)*i,this.y=e.y+(t.y-e.y)*i,this}equals(e){return e.x===this.x&&e.y===this.y}fromArray(e,t=0){return this.x=e[t],this.y=e[t+1],this}toArray(e=[],t=0){return e[t]=this.x,e[t+1]=this.y,e}fromBufferAttribute(e,t){return this.x=e.getX(t),this.y=e.getY(t),this}rotateAround(e,t){let i=Math.cos(t),r=Math.sin(t),s=this.x-e.x,o=this.y-e.y;return this.x=s*i-o*r+e.x,this.y=s*r+o*i+e.y,this}random(){return this.x=Math.random(),this.y=Math.random(),this}*[Symbol.iterator](){yield this.x,yield this.y}},Di=class{constructor(e=0,t=0,i=0,r=1){this.isQuaternion=!0,this._x=e,this._y=t,this._z=i,this._w=r}static slerpFlat(e,t,i,r,s,o,a){let c=i[r+0],l=i[r+1],u=i[r+2],f=i[r+3],d=s[o+0],h=s[o+1],g=s[o+2],x=s[o+3];if(f!==x||c!==d||l!==h||u!==g){let m=c*d+l*h+u*g+f*x;m<0&&(d=-d,h=-h,g=-g,x=-x,m=-m);let p=1-a;if(m<.9995){let M=Math.acos(m),T=Math.sin(M);p=Math.sin(p*M)/T,a=Math.sin(a*M)/T,c=c*p+d*a,l=l*p+h*a,u=u*p+g*a,f=f*p+x*a}else{c=c*p+d*a,l=l*p+h*a,u=u*p+g*a,f=f*p+x*a;let M=1/Math.sqrt(c*c+l*l+u*u+f*f);c*=M,l*=M,u*=M,f*=M}}e[t]=c,e[t+1]=l,e[t+2]=u,e[t+3]=f}static multiplyQuaternionsFlat(e,t,i,r,s,o){let a=i[r],c=i[r+1],l=i[r+2],u=i[r+3],f=s[o],d=s[o+1],h=s[o+2],g=s[o+3];return e[t]=a*g+u*f+c*h-l*d,e[t+1]=c*g+u*d+l*f-a*h,e[t+2]=l*g+u*h+a*d-c*f,e[t+3]=u*g-a*f-c*d-l*h,e}get x(){return this._x}set x(e){this._x=e,this._onChangeCallback()}get y(){return this._y}set y(e){this._y=e,this._onChangeCallback()}get z(){return this._z}set z(e){this._z=e,this._onChangeCallback()}get w(){return this._w}set w(e){this._w=e,this._onChangeCallback()}set(e,t,i,r){return this._x=e,this._y=t,this._z=i,this._w=r,this._onChangeCallback(),this}clone(){return new this.constructor(this._x,this._y,this._z,this._w)}copy(e){return this._x=e.x,this._y=e.y,this._z=e.z,this._w=e.w,this._onChangeCallback(),this}setFromEuler(e,t=!0){let i=e._x,r=e._y,s=e._z,o=e._order,a=Math.cos,c=Math.sin,l=a(i/2),u=a(r/2),f=a(s/2),d=c(i/2),h=c(r/2),g=c(s/2);switch(o){case"XYZ":this._x=d*u*f+l*h*g,this._y=l*h*f-d*u*g,this._z=l*u*g+d*h*f,this._w=l*u*f-d*h*g;break;case"YXZ":this._x=d*u*f+l*h*g,this._y=l*h*f-d*u*g,this._z=l*u*g-d*h*f,this._w=l*u*f+d*h*g;break;case"ZXY":this._x=d*u*f-l*h*g,this._y=l*h*f+d*u*g,this._z=l*u*g+d*h*f,this._w=l*u*f-d*h*g;break;case"ZYX":this._x=d*u*f-l*h*g,this._y=l*h*f+d*u*g,this._z=l*u*g-d*h*f,this._w=l*u*f+d*h*g;break;case"YZX":this._x=d*u*f+l*h*g,this._y=l*h*f+d*u*g,this._z=l*u*g-d*h*f,this._w=l*u*f-d*h*g;break;case"XZY":this._x=d*u*f-l*h*g,this._y=l*h*f-d*u*g,this._z=l*u*g+d*h*f,this._w=l*u*f+d*h*g;break;default:Le("Quaternion: .setFromEuler() encountered an unknown order: "+o)}return t===!0&&this._onChangeCallback(),this}setFromAxisAngle(e,t){let i=t/2,r=Math.sin(i);return this._x=e.x*r,this._y=e.y*r,this._z=e.z*r,this._w=Math.cos(i),this._onChangeCallback(),this}setFromRotationMatrix(e){let t=e.elements,i=t[0],r=t[4],s=t[8],o=t[1],a=t[5],c=t[9],l=t[2],u=t[6],f=t[10],d=i+a+f;if(d>0){let h=.5/Math.sqrt(d+1);this._w=.25/h,this._x=(u-c)*h,this._y=(s-l)*h,this._z=(o-r)*h}else if(i>a&&i>f){let h=2*Math.sqrt(1+i-a-f);this._w=(u-c)/h,this._x=.25*h,this._y=(r+o)/h,this._z=(s+l)/h}else if(a>f){let h=2*Math.sqrt(1+a-i-f);this._w=(s-l)/h,this._x=(r+o)/h,this._y=.25*h,this._z=(c+u)/h}else{let h=2*Math.sqrt(1+f-i-a);this._w=(o-r)/h,this._x=(s+l)/h,this._y=(c+u)/h,this._z=.25*h}return this._onChangeCallback(),this}setFromUnitVectors(e,t){let i=e.dot(t)+1;return i<1e-8?(i=0,Math.abs(e.x)>Math.abs(e.z)?(this._x=-e.y,this._y=e.x,this._z=0,this._w=i):(this._x=0,this._y=-e.z,this._z=e.y,this._w=i)):(this._x=e.y*t.z-e.z*t.y,this._y=e.z*t.x-e.x*t.z,this._z=e.x*t.y-e.y*t.x,this._w=i),this.normalize()}angleTo(e){return 2*Math.acos(Math.abs(Ke(this.dot(e),-1,1)))}rotateTowards(e,t){let i=this.angleTo(e);if(i===0)return this;let r=Math.min(1,t/i);return this.slerp(e,r),this}identity(){return this.set(0,0,0,1)}invert(){return this.conjugate()}conjugate(){return this._x*=-1,this._y*=-1,this._z*=-1,this._onChangeCallback(),this}dot(e){return this._x*e._x+this._y*e._y+this._z*e._z+this._w*e._w}lengthSq(){return this._x*this._x+this._y*this._y+this._z*this._z+this._w*this._w}length(){return Math.sqrt(this._x*this._x+this._y*this._y+this._z*this._z+this._w*this._w)}normalize(){let e=this.length();return e===0?(this._x=0,this._y=0,this._z=0,this._w=1):(e=1/e,this._x=this._x*e,this._y=this._y*e,this._z=this._z*e,this._w=this._w*e),this._onChangeCallback(),this}multiply(e){return this.multiplyQuaternions(this,e)}premultiply(e){return this.multiplyQuaternions(e,this)}multiplyQuaternions(e,t){let i=e._x,r=e._y,s=e._z,o=e._w,a=t._x,c=t._y,l=t._z,u=t._w;return this._x=i*u+o*a+r*l-s*c,this._y=r*u+o*c+s*a-i*l,this._z=s*u+o*l+i*c-r*a,this._w=o*u-i*a-r*c-s*l,this._onChangeCallback(),this}slerp(e,t){let i=e._x,r=e._y,s=e._z,o=e._w,a=this.dot(e);a<0&&(i=-i,r=-r,s=-s,o=-o,a=-a);let c=1-t;if(a<.9995){let l=Math.acos(a),u=Math.sin(l);c=Math.sin(c*l)/u,t=Math.sin(t*l)/u,this._x=this._x*c+i*t,this._y=this._y*c+r*t,this._z=this._z*c+s*t,this._w=this._w*c+o*t,this._onChangeCallback()}else this._x=this._x*c+i*t,this._y=this._y*c+r*t,this._z=this._z*c+s*t,this._w=this._w*c+o*t,this.normalize();return this}slerpQuaternions(e,t,i){return this.copy(e).slerp(t,i)}random(){let e=2*Math.PI*Math.random(),t=2*Math.PI*Math.random(),i=Math.random(),r=Math.sqrt(1-i),s=Math.sqrt(i);return this.set(r*Math.sin(e),r*Math.cos(e),s*Math.sin(t),s*Math.cos(t))}equals(e){return e._x===this._x&&e._y===this._y&&e._z===this._z&&e._w===this._w}fromArray(e,t=0){return this._x=e[t],this._y=e[t+1],this._z=e[t+2],this._w=e[t+3],this._onChangeCallback(),this}toArray(e=[],t=0){return e[t]=this._x,e[t+1]=this._y,e[t+2]=this._z,e[t+3]=this._w,e}fromBufferAttribute(e,t){return this._x=e.getX(t),this._y=e.getY(t),this._z=e.getZ(t),this._w=e.getW(t),this._onChangeCallback(),this}toJSON(){return this.toArray()}_onChange(e){return this._onChangeCallback=e,this}_onChangeCallback(){}*[Symbol.iterator](){yield this._x,yield this._y,yield this._z,yield this._w}},k=class n{constructor(e=0,t=0,i=0){n.prototype.isVector3=!0,this.x=e,this.y=t,this.z=i}set(e,t,i){return i===void 0&&(i=this.z),this.x=e,this.y=t,this.z=i,this}setScalar(e){return this.x=e,this.y=e,this.z=e,this}setX(e){return this.x=e,this}setY(e){return this.y=e,this}setZ(e){return this.z=e,this}setComponent(e,t){switch(e){case 0:this.x=t;break;case 1:this.y=t;break;case 2:this.z=t;break;default:throw new Error("index is out of range: "+e)}return this}getComponent(e){switch(e){case 0:return this.x;case 1:return this.y;case 2:return this.z;default:throw new Error("index is out of range: "+e)}}clone(){return new this.constructor(this.x,this.y,this.z)}copy(e){return this.x=e.x,this.y=e.y,this.z=e.z,this}add(e){return this.x+=e.x,this.y+=e.y,this.z+=e.z,this}addScalar(e){return this.x+=e,this.y+=e,this.z+=e,this}addVectors(e,t){return this.x=e.x+t.x,this.y=e.y+t.y,this.z=e.z+t.z,this}addScaledVector(e,t){return this.x+=e.x*t,this.y+=e.y*t,this.z+=e.z*t,this}sub(e){return this.x-=e.x,this.y-=e.y,this.z-=e.z,this}subScalar(e){return this.x-=e,this.y-=e,this.z-=e,this}subVectors(e,t){return this.x=e.x-t.x,this.y=e.y-t.y,this.z=e.z-t.z,this}multiply(e){return this.x*=e.x,this.y*=e.y,this.z*=e.z,this}multiplyScalar(e){return this.x*=e,this.y*=e,this.z*=e,this}multiplyVectors(e,t){return this.x=e.x*t.x,this.y=e.y*t.y,this.z=e.z*t.z,this}applyEuler(e){return this.applyQuaternion(K0.setFromEuler(e))}applyAxisAngle(e,t){return this.applyQuaternion(K0.setFromAxisAngle(e,t))}applyMatrix3(e){let t=this.x,i=this.y,r=this.z,s=e.elements;return this.x=s[0]*t+s[3]*i+s[6]*r,this.y=s[1]*t+s[4]*i+s[7]*r,this.z=s[2]*t+s[5]*i+s[8]*r,this}applyNormalMatrix(e){return this.applyMatrix3(e).normalize()}applyMatrix4(e){let t=this.x,i=this.y,r=this.z,s=e.elements,o=1/(s[3]*t+s[7]*i+s[11]*r+s[15]);return this.x=(s[0]*t+s[4]*i+s[8]*r+s[12])*o,this.y=(s[1]*t+s[5]*i+s[9]*r+s[13])*o,this.z=(s[2]*t+s[6]*i+s[10]*r+s[14])*o,this}applyQuaternion(e){let t=this.x,i=this.y,r=this.z,s=e.x,o=e.y,a=e.z,c=e.w,l=2*(o*r-a*i),u=2*(a*t-s*r),f=2*(s*i-o*t);return this.x=t+c*l+o*f-a*u,this.y=i+c*u+a*l-s*f,this.z=r+c*f+s*u-o*l,this}project(e){return this.applyMatrix4(e.matrixWorldInverse).applyMatrix4(e.projectionMatrix)}unproject(e){return this.applyMatrix4(e.projectionMatrixInverse).applyMatrix4(e.matrixWorld)}transformDirection(e){let t=this.x,i=this.y,r=this.z,s=e.elements;return this.x=s[0]*t+s[4]*i+s[8]*r,this.y=s[1]*t+s[5]*i+s[9]*r,this.z=s[2]*t+s[6]*i+s[10]*r,this.normalize()}divide(e){return this.x/=e.x,this.y/=e.y,this.z/=e.z,this}divideScalar(e){return this.multiplyScalar(1/e)}min(e){return this.x=Math.min(this.x,e.x),this.y=Math.min(this.y,e.y),this.z=Math.min(this.z,e.z),this}max(e){return this.x=Math.max(this.x,e.x),this.y=Math.max(this.y,e.y),this.z=Math.max(this.z,e.z),this}clamp(e,t){return this.x=Ke(this.x,e.x,t.x),this.y=Ke(this.y,e.y,t.y),this.z=Ke(this.z,e.z,t.z),this}clampScalar(e,t){return this.x=Ke(this.x,e,t),this.y=Ke(this.y,e,t),this.z=Ke(this.z,e,t),this}clampLength(e,t){let i=this.length();return this.divideScalar(i||1).multiplyScalar(Ke(i,e,t))}floor(){return this.x=Math.floor(this.x),this.y=Math.floor(this.y),this.z=Math.floor(this.z),this}ceil(){return this.x=Math.ceil(this.x),this.y=Math.ceil(this.y),this.z=Math.ceil(this.z),this}round(){return this.x=Math.round(this.x),this.y=Math.round(this.y),this.z=Math.round(this.z),this}roundToZero(){return this.x=Math.trunc(this.x),this.y=Math.trunc(this.y),this.z=Math.trunc(this.z),this}negate(){return this.x=-this.x,this.y=-this.y,this.z=-this.z,this}dot(e){return this.x*e.x+this.y*e.y+this.z*e.z}lengthSq(){return this.x*this.x+this.y*this.y+this.z*this.z}length(){return Math.sqrt(this.x*this.x+this.y*this.y+this.z*this.z)}manhattanLength(){return Math.abs(this.x)+Math.abs(this.y)+Math.abs(this.z)}normalize(){return this.divideScalar(this.length()||1)}setLength(e){return this.normalize().multiplyScalar(e)}lerp(e,t){return this.x+=(e.x-this.x)*t,this.y+=(e.y-this.y)*t,this.z+=(e.z-this.z)*t,this}lerpVectors(e,t,i){return this.x=e.x+(t.x-e.x)*i,this.y=e.y+(t.y-e.y)*i,this.z=e.z+(t.z-e.z)*i,this}cross(e){return this.crossVectors(this,e)}crossVectors(e,t){let i=e.x,r=e.y,s=e.z,o=t.x,a=t.y,c=t.z;return this.x=r*c-s*a,this.y=s*o-i*c,this.z=i*a-r*o,this}projectOnVector(e){let t=e.lengthSq();if(t===0)return this.set(0,0,0);let i=e.dot(this)/t;return this.copy(e).multiplyScalar(i)}projectOnPlane(e){return _m.copy(this).projectOnVector(e),this.sub(_m)}reflect(e){return this.sub(_m.copy(e).multiplyScalar(2*this.dot(e)))}angleTo(e){let t=Math.sqrt(this.lengthSq()*e.lengthSq());if(t===0)return Math.PI/2;let i=this.dot(e)/t;return Math.acos(Ke(i,-1,1))}distanceTo(e){return Math.sqrt(this.distanceToSquared(e))}distanceToSquared(e){let t=this.x-e.x,i=this.y-e.y,r=this.z-e.z;return t*t+i*i+r*r}manhattanDistanceTo(e){return Math.abs(this.x-e.x)+Math.abs(this.y-e.y)+Math.abs(this.z-e.z)}setFromSpherical(e){return this.setFromSphericalCoords(e.radius,e.phi,e.theta)}setFromSphericalCoords(e,t,i){let r=Math.sin(t)*e;return this.x=r*Math.sin(i),this.y=Math.cos(t)*e,this.z=r*Math.cos(i),this}setFromCylindrical(e){return this.setFromCylindricalCoords(e.radius,e.theta,e.y)}setFromCylindricalCoords(e,t,i){return this.x=e*Math.sin(t),this.y=i,this.z=e*Math.cos(t),this}setFromMatrixPosition(e){let t=e.elements;return this.x=t[12],this.y=t[13],this.z=t[14],this}setFromMatrixScale(e){let t=this.setFromMatrixColumn(e,0).length(),i=this.setFromMatrixColumn(e,1).length(),r=this.setFromMatrixColumn(e,2).length();return this.x=t,this.y=i,this.z=r,this}setFromMatrixColumn(e,t){return this.fromArray(e.elements,t*4)}setFromMatrix3Column(e,t){return this.fromArray(e.elements,t*3)}setFromEuler(e){return this.x=e._x,this.y=e._y,this.z=e._z,this}setFromColor(e){return this.x=e.r,this.y=e.g,this.z=e.b,this}equals(e){return e.x===this.x&&e.y===this.y&&e.z===this.z}fromArray(e,t=0){return this.x=e[t],this.y=e[t+1],this.z=e[t+2],this}toArray(e=[],t=0){return e[t]=this.x,e[t+1]=this.y,e[t+2]=this.z,e}fromBufferAttribute(e,t){return this.x=e.getX(t),this.y=e.getY(t),this.z=e.getZ(t),this}random(){return this.x=Math.random(),this.y=Math.random(),this.z=Math.random(),this}randomDirection(){let e=Math.random()*Math.PI*2,t=Math.random()*2-1,i=Math.sqrt(1-t*t);return this.x=i*Math.cos(e),this.y=t,this.z=i*Math.sin(e),this}*[Symbol.iterator](){yield this.x,yield this.y,yield this.z}},_m=new k,K0=new Di,Be=class n{constructor(e,t,i,r,s,o,a,c,l){n.prototype.isMatrix3=!0,this.elements=[1,0,0,0,1,0,0,0,1],e!==void 0&&this.set(e,t,i,r,s,o,a,c,l)}set(e,t,i,r,s,o,a,c,l){let u=this.elements;return u[0]=e,u[1]=r,u[2]=a,u[3]=t,u[4]=s,u[5]=c,u[6]=i,u[7]=o,u[8]=l,this}identity(){return this.set(1,0,0,0,1,0,0,0,1),this}copy(e){let t=this.elements,i=e.elements;return t[0]=i[0],t[1]=i[1],t[2]=i[2],t[3]=i[3],t[4]=i[4],t[5]=i[5],t[6]=i[6],t[7]=i[7],t[8]=i[8],this}extractBasis(e,t,i){return e.setFromMatrix3Column(this,0),t.setFromMatrix3Column(this,1),i.setFromMatrix3Column(this,2),this}setFromMatrix4(e){let t=e.elements;return this.set(t[0],t[4],t[8],t[1],t[5],t[9],t[2],t[6],t[10]),this}multiply(e){return this.multiplyMatrices(this,e)}premultiply(e){return this.multiplyMatrices(e,this)}multiplyMatrices(e,t){let i=e.elements,r=t.elements,s=this.elements,o=i[0],a=i[3],c=i[6],l=i[1],u=i[4],f=i[7],d=i[2],h=i[5],g=i[8],x=r[0],m=r[3],p=r[6],M=r[1],T=r[4],S=r[7],I=r[2],D=r[5],A=r[8];return s[0]=o*x+a*M+c*I,s[3]=o*m+a*T+c*D,s[6]=o*p+a*S+c*A,s[1]=l*x+u*M+f*I,s[4]=l*m+u*T+f*D,s[7]=l*p+u*S+f*A,s[2]=d*x+h*M+g*I,s[5]=d*m+h*T+g*D,s[8]=d*p+h*S+g*A,this}multiplyScalar(e){let t=this.elements;return t[0]*=e,t[3]*=e,t[6]*=e,t[1]*=e,t[4]*=e,t[7]*=e,t[2]*=e,t[5]*=e,t[8]*=e,this}determinant(){let e=this.elements,t=e[0],i=e[1],r=e[2],s=e[3],o=e[4],a=e[5],c=e[6],l=e[7],u=e[8];return t*o*u-t*a*l-i*s*u+i*a*c+r*s*l-r*o*c}invert(){let e=this.elements,t=e[0],i=e[1],r=e[2],s=e[3],o=e[4],a=e[5],c=e[6],l=e[7],u=e[8],f=u*o-a*l,d=a*c-u*s,h=l*s-o*c,g=t*f+i*d+r*h;if(g===0)return this.set(0,0,0,0,0,0,0,0,0);let x=1/g;return e[0]=f*x,e[1]=(r*l-u*i)*x,e[2]=(a*i-r*o)*x,e[3]=d*x,e[4]=(u*t-r*c)*x,e[5]=(r*s-a*t)*x,e[6]=h*x,e[7]=(i*c-l*t)*x,e[8]=(o*t-i*s)*x,this}transpose(){let e,t=this.elements;return e=t[1],t[1]=t[3],t[3]=e,e=t[2],t[2]=t[6],t[6]=e,e=t[5],t[5]=t[7],t[7]=e,this}getNormalMatrix(e){return this.setFromMatrix4(e).invert().transpose()}transposeIntoArray(e){let t=this.elements;return e[0]=t[0],e[1]=t[3],e[2]=t[6],e[3]=t[1],e[4]=t[4],e[5]=t[7],e[6]=t[2],e[7]=t[5],e[8]=t[8],this}setUvTransform(e,t,i,r,s,o,a){let c=Math.cos(s),l=Math.sin(s);return this.set(i*c,i*l,-i*(c*o+l*a)+o+e,-r*l,r*c,-r*(-l*o+c*a)+a+t,0,0,1),this}scale(e,t){return this.premultiply(xm.makeScale(e,t)),this}rotate(e){return this.premultiply(xm.makeRotation(-e)),this}translate(e,t){return this.premultiply(xm.makeTranslation(e,t)),this}makeTranslation(e,t){return e.isVector2?this.set(1,0,e.x,0,1,e.y,0,0,1):this.set(1,0,e,0,1,t,0,0,1),this}makeRotation(e){let t=Math.cos(e),i=Math.sin(e);return this.set(t,-i,0,i,t,0,0,0,1),this}makeScale(e,t){return this.set(e,0,0,0,t,0,0,0,1),this}equals(e){let t=this.elements,i=e.elements;for(let r=0;r<9;r++)if(t[r]!==i[r])return!1;return!0}fromArray(e,t=0){for(let i=0;i<9;i++)this.elements[i]=e[i+t];return this}toArray(e=[],t=0){let i=this.elements;return e[t]=i[0],e[t+1]=i[1],e[t+2]=i[2],e[t+3]=i[3],e[t+4]=i[4],e[t+5]=i[5],e[t+6]=i[6],e[t+7]=i[7],e[t+8]=i[8],e}clone(){return new this.constructor().fromArray(this.elements)}},xm=new Be,Q0=new Be().set(.4123908,.3575843,.1804808,.212639,.7151687,.0721923,.0193308,.1191948,.9505322),ex=new Be().set(3.2409699,-1.5373832,-.4986108,-.9692436,1.8759675,.0415551,.0556301,-.203977,1.0569715);function Aw(){let n={enabled:!0,workingColorSpace:Es,spaces:{},convert:function(r,s,o){return this.enabled===!1||s===o||!s||!o||(this.spaces[s].transfer===dt&&(r.r=nr(r.r),r.g=nr(r.g),r.b=nr(r.b)),this.spaces[s].primaries!==this.spaces[o].primaries&&(r.applyMatrix3(this.spaces[s].toXYZ),r.applyMatrix3(this.spaces[o].fromXYZ)),this.spaces[o].transfer===dt&&(r.r=To(r.r),r.g=To(r.g),r.b=To(r.b))),r},workingToColorSpace:function(r,s){return this.convert(r,this.workingColorSpace,s)},colorSpaceToWorking:function(r,s){return this.convert(r,s,this.workingColorSpace)},getPrimaries:function(r){return this.spaces[r].primaries},getTransfer:function(r){return r===or?Ua:this.spaces[r].transfer},getToneMappingMode:function(r){return this.spaces[r].outputColorSpaceConfig.toneMappingMode||"standard"},getLuminanceCoefficients:function(r,s=this.workingColorSpace){return r.fromArray(this.spaces[s].luminanceCoefficients)},define:function(r){Object.assign(this.spaces,r)},_getMatrix:function(r,s,o){return r.copy(this.spaces[s].toXYZ).multiply(this.spaces[o].fromXYZ)},_getDrawingBufferColorSpace:function(r){return this.spaces[r].outputColorSpaceConfig.drawingBufferColorSpace},_getUnpackColorSpace:function(r=this.workingColorSpace){return this.spaces[r].workingColorSpaceConfig.unpackColorSpace},fromWorkingColorSpace:function(r,s){return Ha("ColorManagement: .fromWorkingColorSpace() has been renamed to .workingToColorSpace()."),n.workingToColorSpace(r,s)},toWorkingColorSpace:function(r,s){return Ha("ColorManagement: .toWorkingColorSpace() has been renamed to .colorSpaceToWorking()."),n.colorSpaceToWorking(r,s)}},e=[.64,.33,.3,.6,.15,.06],t=[.2126,.7152,.0722],i=[.3127,.329];return n.define({[Es]:{primaries:e,whitePoint:i,transfer:Ua,toXYZ:Q0,fromXYZ:ex,luminanceCoefficients:t,workingColorSpaceConfig:{unpackColorSpace:Pn},outputColorSpaceConfig:{drawingBufferColorSpace:Pn}},[Pn]:{primaries:e,whitePoint:i,transfer:dt,toXYZ:Q0,fromXYZ:ex,luminanceCoefficients:t,outputColorSpaceConfig:{drawingBufferColorSpace:Pn}}}),n}var rt=Aw();function nr(n){return n<.04045?n*.0773993808:Math.pow(n*.9478672986+.0521327014,2.4)}function To(n){return n<.0031308?n*12.92:1.055*Math.pow(n,.41666)-.055}var ho,Vu=class{static getDataURL(e,t="image/png"){if(/^data:/i.test(e.src)||typeof HTMLCanvasElement>"u")return e.src;let i;if(e instanceof HTMLCanvasElement)i=e;else{ho===void 0&&(ho=Va("canvas")),ho.width=e.width,ho.height=e.height;let r=ho.getContext("2d");e instanceof ImageData?r.putImageData(e,0,0):r.drawImage(e,0,0,e.width,e.height),i=ho}return i.toDataURL(t)}static sRGBToLinear(e){if(typeof HTMLImageElement<"u"&&e instanceof HTMLImageElement||typeof HTMLCanvasElement<"u"&&e instanceof HTMLCanvasElement||typeof ImageBitmap<"u"&&e instanceof ImageBitmap){let t=Va("canvas");t.width=e.width,t.height=e.height;let i=t.getContext("2d");i.drawImage(e,0,0,e.width,e.height);let r=i.getImageData(0,0,e.width,e.height),s=r.data;for(let o=0;o<s.length;o++)s[o]=nr(s[o]/255)*255;return i.putImageData(r,0,0),t}else if(e.data){let t=e.data.slice(0);for(let i=0;i<t.length;i++)t instanceof Uint8Array||t instanceof Uint8ClampedArray?t[i]=Math.floor(nr(t[i]/255)*255):t[i]=nr(t[i]);return{data:t,width:e.width,height:e.height}}else return Le("ImageUtils.sRGBToLinear(): Unsupported image type. No color space conversion applied."),e}},Rw=0,wo=class{constructor(e=null){this.isSource=!0,Object.defineProperty(this,"id",{value:Rw++}),this.uuid=dc(),this.data=e,this.dataReady=!0,this.version=0}getSize(e){let t=this.data;return typeof HTMLVideoElement<"u"&&t instanceof HTMLVideoElement?e.set(t.videoWidth,t.videoHeight,0):typeof VideoFrame<"u"&&t instanceof VideoFrame?e.set(t.displayHeight,t.displayWidth,0):t!==null?e.set(t.width,t.height,t.depth||0):e.set(0,0,0),e}set needsUpdate(e){e===!0&&this.version++}toJSON(e){let t=e===void 0||typeof e=="string";if(!t&&e.images[this.uuid]!==void 0)return e.images[this.uuid];let i={uuid:this.uuid,url:""},r=this.data;if(r!==null){let s;if(Array.isArray(r)){s=[];for(let o=0,a=r.length;o<a;o++)r[o].isDataTexture?s.push(Mm(r[o].image)):s.push(Mm(r[o]))}else s=Mm(r);i.url=s}return t||(e.images[this.uuid]=i),i}};function Mm(n){return typeof HTMLImageElement<"u"&&n instanceof HTMLImageElement||typeof HTMLCanvasElement<"u"&&n instanceof HTMLCanvasElement||typeof ImageBitmap<"u"&&n instanceof ImageBitmap?Vu.getDataURL(n):n.data?{data:Array.from(n.data),width:n.width,height:n.height,type:n.data.constructor.name}:(Le("Texture: Unable to serialize Texture."),{})}var Nw=0,bm=new k,ar=(()=>{class n extends sr{constructor(t=n.DEFAULT_IMAGE,i=n.DEFAULT_MAPPING,r=Ci,s=Ci,o=sn,a=Vr,c=Jn,l=Un,u=n.DEFAULT_ANISOTROPY,f=or){super(),this.isTexture=!0,Object.defineProperty(this,"id",{value:Nw++}),this.uuid=dc(),this.name="",this.source=new wo(t),this.mipmaps=[],this.mapping=i,this.channel=0,this.wrapS=r,this.wrapT=s,this.magFilter=o,this.minFilter=a,this.anisotropy=u,this.format=c,this.internalFormat=null,this.type=l,this.offset=new pt(0,0),this.repeat=new pt(1,1),this.center=new pt(0,0),this.rotation=0,this.matrixAutoUpdate=!0,this.matrix=new Be,this.generateMipmaps=!0,this.premultiplyAlpha=!1,this.flipY=!0,this.unpackAlignment=4,this.colorSpace=f,this.userData={},this.updateRanges=[],this.version=0,this.onUpdate=null,this.renderTarget=null,this.isRenderTargetTexture=!1,this.isArrayTexture=!!(t&&t.depth&&t.depth>1),this.pmremVersion=0}get width(){return this.source.getSize(bm).x}get height(){return this.source.getSize(bm).y}get depth(){return this.source.getSize(bm).z}get image(){return this.source.data}set image(t=null){this.source.data=t}updateMatrix(){this.matrix.setUvTransform(this.offset.x,this.offset.y,this.repeat.x,this.repeat.y,this.rotation,this.center.x,this.center.y)}addUpdateRange(t,i){this.updateRanges.push({start:t,count:i})}clearUpdateRanges(){this.updateRanges.length=0}clone(){return new this.constructor().copy(this)}copy(t){return this.name=t.name,this.source=t.source,this.mipmaps=t.mipmaps.slice(0),this.mapping=t.mapping,this.channel=t.channel,this.wrapS=t.wrapS,this.wrapT=t.wrapT,this.magFilter=t.magFilter,this.minFilter=t.minFilter,this.anisotropy=t.anisotropy,this.format=t.format,this.internalFormat=t.internalFormat,this.type=t.type,this.offset.copy(t.offset),this.repeat.copy(t.repeat),this.center.copy(t.center),this.rotation=t.rotation,this.matrixAutoUpdate=t.matrixAutoUpdate,this.matrix.copy(t.matrix),this.generateMipmaps=t.generateMipmaps,this.premultiplyAlpha=t.premultiplyAlpha,this.flipY=t.flipY,this.unpackAlignment=t.unpackAlignment,this.colorSpace=t.colorSpace,this.renderTarget=t.renderTarget,this.isRenderTargetTexture=t.isRenderTargetTexture,this.isArrayTexture=t.isArrayTexture,this.userData=JSON.parse(JSON.stringify(t.userData)),this.needsUpdate=!0,this}setValues(t){for(let i in t){let r=t[i];if(r===void 0){Le(`Texture.setValues(): parameter '${i}' has value of undefined.`);continue}let s=this[i];if(s===void 0){Le(`Texture.setValues(): property '${i}' does not exist.`);continue}s&&r&&s.isVector2&&r.isVector2||s&&r&&s.isVector3&&r.isVector3||s&&r&&s.isMatrix3&&r.isMatrix3?s.copy(r):this[i]=r}}toJSON(t){let i=t===void 0||typeof t=="string";if(!i&&t.textures[this.uuid]!==void 0)return t.textures[this.uuid];let r={metadata:{version:4.7,type:"Texture",generator:"Texture.toJSON"},uuid:this.uuid,name:this.name,image:this.source.toJSON(t).uuid,mapping:this.mapping,channel:this.channel,repeat:[this.repeat.x,this.repeat.y],offset:[this.offset.x,this.offset.y],center:[this.center.x,this.center.y],rotation:this.rotation,wrap:[this.wrapS,this.wrapT],format:this.format,internalFormat:this.internalFormat,type:this.type,colorSpace:this.colorSpace,minFilter:this.minFilter,magFilter:this.magFilter,anisotropy:this.anisotropy,flipY:this.flipY,generateMipmaps:this.generateMipmaps,premultiplyAlpha:this.premultiplyAlpha,unpackAlignment:this.unpackAlignment};return Object.keys(this.userData).length>0&&(r.userData=this.userData),i||(t.textures[this.uuid]=r),r}dispose(){this.dispatchEvent({type:"dispose"})}transformUv(t){if(this.mapping!==Vm)return t;if(t.applyMatrix3(this.matrix),t.x<0||t.x>1)switch(this.wrapS){case Ou:t.x=t.x-Math.floor(t.x);break;case Ci:t.x=t.x<0?0:1;break;case ku:Math.abs(Math.floor(t.x)%2)===1?t.x=Math.ceil(t.x)-t.x:t.x=t.x-Math.floor(t.x);break}if(t.y<0||t.y>1)switch(this.wrapT){case Ou:t.y=t.y-Math.floor(t.y);break;case Ci:t.y=t.y<0?0:1;break;case ku:Math.abs(Math.floor(t.y)%2)===1?t.y=Math.ceil(t.y)-t.y:t.y=t.y-Math.floor(t.y);break}return this.flipY&&(t.y=1-t.y),t}set needsUpdate(t){t===!0&&(this.version++,this.source.needsUpdate=!0)}set needsPMREMUpdate(t){t===!0&&this.pmremVersion++}}return n.DEFAULT_IMAGE=null,n.DEFAULT_MAPPING=Vm,n.DEFAULT_ANISOTROPY=1,n})(),Lt=class n{constructor(e=0,t=0,i=0,r=1){n.prototype.isVector4=!0,this.x=e,this.y=t,this.z=i,this.w=r}get width(){return this.z}set width(e){this.z=e}get height(){return this.w}set height(e){this.w=e}set(e,t,i,r){return this.x=e,this.y=t,this.z=i,this.w=r,this}setScalar(e){return this.x=e,this.y=e,this.z=e,this.w=e,this}setX(e){return this.x=e,this}setY(e){return this.y=e,this}setZ(e){return this.z=e,this}setW(e){return this.w=e,this}setComponent(e,t){switch(e){case 0:this.x=t;break;case 1:this.y=t;break;case 2:this.z=t;break;case 3:this.w=t;break;default:throw new Error("index is out of range: "+e)}return this}getComponent(e){switch(e){case 0:return this.x;case 1:return this.y;case 2:return this.z;case 3:return this.w;default:throw new Error("index is out of range: "+e)}}clone(){return new this.constructor(this.x,this.y,this.z,this.w)}copy(e){return this.x=e.x,this.y=e.y,this.z=e.z,this.w=e.w!==void 0?e.w:1,this}add(e){return this.x+=e.x,this.y+=e.y,this.z+=e.z,this.w+=e.w,this}addScalar(e){return this.x+=e,this.y+=e,this.z+=e,this.w+=e,this}addVectors(e,t){return this.x=e.x+t.x,this.y=e.y+t.y,this.z=e.z+t.z,this.w=e.w+t.w,this}addScaledVector(e,t){return this.x+=e.x*t,this.y+=e.y*t,this.z+=e.z*t,this.w+=e.w*t,this}sub(e){return this.x-=e.x,this.y-=e.y,this.z-=e.z,this.w-=e.w,this}subScalar(e){return this.x-=e,this.y-=e,this.z-=e,this.w-=e,this}subVectors(e,t){return this.x=e.x-t.x,this.y=e.y-t.y,this.z=e.z-t.z,this.w=e.w-t.w,this}multiply(e){return this.x*=e.x,this.y*=e.y,this.z*=e.z,this.w*=e.w,this}multiplyScalar(e){return this.x*=e,this.y*=e,this.z*=e,this.w*=e,this}applyMatrix4(e){let t=this.x,i=this.y,r=this.z,s=this.w,o=e.elements;return this.x=o[0]*t+o[4]*i+o[8]*r+o[12]*s,this.y=o[1]*t+o[5]*i+o[9]*r+o[13]*s,this.z=o[2]*t+o[6]*i+o[10]*r+o[14]*s,this.w=o[3]*t+o[7]*i+o[11]*r+o[15]*s,this}divide(e){return this.x/=e.x,this.y/=e.y,this.z/=e.z,this.w/=e.w,this}divideScalar(e){return this.multiplyScalar(1/e)}setAxisAngleFromQuaternion(e){this.w=2*Math.acos(e.w);let t=Math.sqrt(1-e.w*e.w);return t<1e-4?(this.x=1,this.y=0,this.z=0):(this.x=e.x/t,this.y=e.y/t,this.z=e.z/t),this}setAxisAngleFromRotationMatrix(e){let t,i,r,s,c=e.elements,l=c[0],u=c[4],f=c[8],d=c[1],h=c[5],g=c[9],x=c[2],m=c[6],p=c[10];if(Math.abs(u-d)<.01&&Math.abs(f-x)<.01&&Math.abs(g-m)<.01){if(Math.abs(u+d)<.1&&Math.abs(f+x)<.1&&Math.abs(g+m)<.1&&Math.abs(l+h+p-3)<.1)return this.set(1,0,0,0),this;t=Math.PI;let T=(l+1)/2,S=(h+1)/2,I=(p+1)/2,D=(u+d)/4,A=(f+x)/4,y=(g+m)/4;return T>S&&T>I?T<.01?(i=0,r=.707106781,s=.707106781):(i=Math.sqrt(T),r=D/i,s=A/i):S>I?S<.01?(i=.707106781,r=0,s=.707106781):(r=Math.sqrt(S),i=D/r,s=y/r):I<.01?(i=.707106781,r=.707106781,s=0):(s=Math.sqrt(I),i=A/s,r=y/s),this.set(i,r,s,t),this}let M=Math.sqrt((m-g)*(m-g)+(f-x)*(f-x)+(d-u)*(d-u));return Math.abs(M)<.001&&(M=1),this.x=(m-g)/M,this.y=(f-x)/M,this.z=(d-u)/M,this.w=Math.acos((l+h+p-1)/2),this}setFromMatrixPosition(e){let t=e.elements;return this.x=t[12],this.y=t[13],this.z=t[14],this.w=t[15],this}min(e){return this.x=Math.min(this.x,e.x),this.y=Math.min(this.y,e.y),this.z=Math.min(this.z,e.z),this.w=Math.min(this.w,e.w),this}max(e){return this.x=Math.max(this.x,e.x),this.y=Math.max(this.y,e.y),this.z=Math.max(this.z,e.z),this.w=Math.max(this.w,e.w),this}clamp(e,t){return this.x=Ke(this.x,e.x,t.x),this.y=Ke(this.y,e.y,t.y),this.z=Ke(this.z,e.z,t.z),this.w=Ke(this.w,e.w,t.w),this}clampScalar(e,t){return this.x=Ke(this.x,e,t),this.y=Ke(this.y,e,t),this.z=Ke(this.z,e,t),this.w=Ke(this.w,e,t),this}clampLength(e,t){let i=this.length();return this.divideScalar(i||1).multiplyScalar(Ke(i,e,t))}floor(){return this.x=Math.floor(this.x),this.y=Math.floor(this.y),this.z=Math.floor(this.z),this.w=Math.floor(this.w),this}ceil(){return this.x=Math.ceil(this.x),this.y=Math.ceil(this.y),this.z=Math.ceil(this.z),this.w=Math.ceil(this.w),this}round(){return this.x=Math.round(this.x),this.y=Math.round(this.y),this.z=Math.round(this.z),this.w=Math.round(this.w),this}roundToZero(){return this.x=Math.trunc(this.x),this.y=Math.trunc(this.y),this.z=Math.trunc(this.z),this.w=Math.trunc(this.w),this}negate(){return this.x=-this.x,this.y=-this.y,this.z=-this.z,this.w=-this.w,this}dot(e){return this.x*e.x+this.y*e.y+this.z*e.z+this.w*e.w}lengthSq(){return this.x*this.x+this.y*this.y+this.z*this.z+this.w*this.w}length(){return Math.sqrt(this.x*this.x+this.y*this.y+this.z*this.z+this.w*this.w)}manhattanLength(){return Math.abs(this.x)+Math.abs(this.y)+Math.abs(this.z)+Math.abs(this.w)}normalize(){return this.divideScalar(this.length()||1)}setLength(e){return this.normalize().multiplyScalar(e)}lerp(e,t){return this.x+=(e.x-this.x)*t,this.y+=(e.y-this.y)*t,this.z+=(e.z-this.z)*t,this.w+=(e.w-this.w)*t,this}lerpVectors(e,t,i){return this.x=e.x+(t.x-e.x)*i,this.y=e.y+(t.y-e.y)*i,this.z=e.z+(t.z-e.z)*i,this.w=e.w+(t.w-e.w)*i,this}equals(e){return e.x===this.x&&e.y===this.y&&e.z===this.z&&e.w===this.w}fromArray(e,t=0){return this.x=e[t],this.y=e[t+1],this.z=e[t+2],this.w=e[t+3],this}toArray(e=[],t=0){return e[t]=this.x,e[t+1]=this.y,e[t+2]=this.z,e[t+3]=this.w,e}fromBufferAttribute(e,t){return this.x=e.getX(t),this.y=e.getY(t),this.z=e.getZ(t),this.w=e.getW(t),this}random(){return this.x=Math.random(),this.y=Math.random(),this.z=Math.random(),this.w=Math.random(),this}*[Symbol.iterator](){yield this.x,yield this.y,yield this.z,yield this.w}},Hu=class extends sr{constructor(e=1,t=1,i={}){super(),i=Object.assign({generateMipmaps:!1,internalFormat:null,minFilter:sn,depthBuffer:!0,stencilBuffer:!1,resolveDepthBuffer:!0,resolveStencilBuffer:!0,depthTexture:null,samples:0,count:1,depth:1,multiview:!1},i),this.isRenderTarget=!0,this.width=e,this.height=t,this.depth=i.depth,this.scissor=new Lt(0,0,e,t),this.scissorTest=!1,this.viewport=new Lt(0,0,e,t),this.textures=[];let r={width:e,height:t,depth:i.depth},s=new ar(r),o=i.count;for(let a=0;a<o;a++)this.textures[a]=s.clone(),this.textures[a].isRenderTargetTexture=!0,this.textures[a].renderTarget=this;this._setTextureOptions(i),this.depthBuffer=i.depthBuffer,this.stencilBuffer=i.stencilBuffer,this.resolveDepthBuffer=i.resolveDepthBuffer,this.resolveStencilBuffer=i.resolveStencilBuffer,this._depthTexture=null,this.depthTexture=i.depthTexture,this.samples=i.samples,this.multiview=i.multiview}_setTextureOptions(e={}){let t={minFilter:sn,generateMipmaps:!1,flipY:!1,internalFormat:null};e.mapping!==void 0&&(t.mapping=e.mapping),e.wrapS!==void 0&&(t.wrapS=e.wrapS),e.wrapT!==void 0&&(t.wrapT=e.wrapT),e.wrapR!==void 0&&(t.wrapR=e.wrapR),e.magFilter!==void 0&&(t.magFilter=e.magFilter),e.minFilter!==void 0&&(t.minFilter=e.minFilter),e.format!==void 0&&(t.format=e.format),e.type!==void 0&&(t.type=e.type),e.anisotropy!==void 0&&(t.anisotropy=e.anisotropy),e.colorSpace!==void 0&&(t.colorSpace=e.colorSpace),e.flipY!==void 0&&(t.flipY=e.flipY),e.generateMipmaps!==void 0&&(t.generateMipmaps=e.generateMipmaps),e.internalFormat!==void 0&&(t.internalFormat=e.internalFormat);for(let i=0;i<this.textures.length;i++)this.textures[i].setValues(t)}get texture(){return this.textures[0]}set texture(e){this.textures[0]=e}set depthTexture(e){this._depthTexture!==null&&(this._depthTexture.renderTarget=null),e!==null&&(e.renderTarget=this),this._depthTexture=e}get depthTexture(){return this._depthTexture}setSize(e,t,i=1){if(this.width!==e||this.height!==t||this.depth!==i){this.width=e,this.height=t,this.depth=i;for(let r=0,s=this.textures.length;r<s;r++)this.textures[r].image.width=e,this.textures[r].image.height=t,this.textures[r].image.depth=i,this.textures[r].isData3DTexture!==!0&&(this.textures[r].isArrayTexture=this.textures[r].image.depth>1);this.dispose()}this.viewport.set(0,0,e,t),this.scissor.set(0,0,e,t)}clone(){return new this.constructor().copy(this)}copy(e){this.width=e.width,this.height=e.height,this.depth=e.depth,this.scissor.copy(e.scissor),this.scissorTest=e.scissorTest,this.viewport.copy(e.viewport),this.textures.length=0;for(let t=0,i=e.textures.length;t<i;t++){this.textures[t]=e.textures[t].clone(),this.textures[t].isRenderTargetTexture=!0,this.textures[t].renderTarget=this;let r=Object.assign({},e.textures[t].image);this.textures[t].source=new wo(r)}return this.depthBuffer=e.depthBuffer,this.stencilBuffer=e.stencilBuffer,this.resolveDepthBuffer=e.resolveDepthBuffer,this.resolveStencilBuffer=e.resolveStencilBuffer,e.depthTexture!==null&&(this.depthTexture=e.depthTexture.clone()),this.samples=e.samples,this}dispose(){this.dispatchEvent({type:"dispose"})}},Ln=class extends Hu{constructor(e=1,t=1,i={}){super(e,t,i),this.isWebGLRenderTarget=!0}},za=class extends ar{constructor(e=null,t=1,i=1,r=1){super(null),this.isDataArrayTexture=!0,this.image={data:e,width:t,height:i,depth:r},this.magFilter=en,this.minFilter=en,this.wrapR=Ci,this.generateMipmaps=!1,this.flipY=!1,this.unpackAlignment=1,this.layerUpdates=new Set}addLayerUpdate(e){this.layerUpdates.add(e)}clearLayerUpdates(){this.layerUpdates.clear()}};var zu=class extends ar{constructor(e=null,t=1,i=1,r=1){super(null),this.isData3DTexture=!0,this.image={data:e,width:t,height:i,depth:r},this.magFilter=en,this.minFilter=en,this.wrapR=Ci,this.generateMipmaps=!1,this.flipY=!1,this.unpackAlignment=1}};var Bt=class n{constructor(e,t,i,r,s,o,a,c,l,u,f,d,h,g,x,m){n.prototype.isMatrix4=!0,this.elements=[1,0,0,0,0,1,0,0,0,0,1,0,0,0,0,1],e!==void 0&&this.set(e,t,i,r,s,o,a,c,l,u,f,d,h,g,x,m)}set(e,t,i,r,s,o,a,c,l,u,f,d,h,g,x,m){let p=this.elements;return p[0]=e,p[4]=t,p[8]=i,p[12]=r,p[1]=s,p[5]=o,p[9]=a,p[13]=c,p[2]=l,p[6]=u,p[10]=f,p[14]=d,p[3]=h,p[7]=g,p[11]=x,p[15]=m,this}identity(){return this.set(1,0,0,0,0,1,0,0,0,0,1,0,0,0,0,1),this}clone(){return new n().fromArray(this.elements)}copy(e){let t=this.elements,i=e.elements;return t[0]=i[0],t[1]=i[1],t[2]=i[2],t[3]=i[3],t[4]=i[4],t[5]=i[5],t[6]=i[6],t[7]=i[7],t[8]=i[8],t[9]=i[9],t[10]=i[10],t[11]=i[11],t[12]=i[12],t[13]=i[13],t[14]=i[14],t[15]=i[15],this}copyPosition(e){let t=this.elements,i=e.elements;return t[12]=i[12],t[13]=i[13],t[14]=i[14],this}setFromMatrix3(e){let t=e.elements;return this.set(t[0],t[3],t[6],0,t[1],t[4],t[7],0,t[2],t[5],t[8],0,0,0,0,1),this}extractBasis(e,t,i){return this.determinant()===0?(e.set(1,0,0),t.set(0,1,0),i.set(0,0,1),this):(e.setFromMatrixColumn(this,0),t.setFromMatrixColumn(this,1),i.setFromMatrixColumn(this,2),this)}makeBasis(e,t,i){return this.set(e.x,t.x,i.x,0,e.y,t.y,i.y,0,e.z,t.z,i.z,0,0,0,0,1),this}extractRotation(e){if(e.determinant()===0)return this.identity();let t=this.elements,i=e.elements,r=1/po.setFromMatrixColumn(e,0).length(),s=1/po.setFromMatrixColumn(e,1).length(),o=1/po.setFromMatrixColumn(e,2).length();return t[0]=i[0]*r,t[1]=i[1]*r,t[2]=i[2]*r,t[3]=0,t[4]=i[4]*s,t[5]=i[5]*s,t[6]=i[6]*s,t[7]=0,t[8]=i[8]*o,t[9]=i[9]*o,t[10]=i[10]*o,t[11]=0,t[12]=0,t[13]=0,t[14]=0,t[15]=1,this}makeRotationFromEuler(e){let t=this.elements,i=e.x,r=e.y,s=e.z,o=Math.cos(i),a=Math.sin(i),c=Math.cos(r),l=Math.sin(r),u=Math.cos(s),f=Math.sin(s);if(e.order==="XYZ"){let d=o*u,h=o*f,g=a*u,x=a*f;t[0]=c*u,t[4]=-c*f,t[8]=l,t[1]=h+g*l,t[5]=d-x*l,t[9]=-a*c,t[2]=x-d*l,t[6]=g+h*l,t[10]=o*c}else if(e.order==="YXZ"){let d=c*u,h=c*f,g=l*u,x=l*f;t[0]=d+x*a,t[4]=g*a-h,t[8]=o*l,t[1]=o*f,t[5]=o*u,t[9]=-a,t[2]=h*a-g,t[6]=x+d*a,t[10]=o*c}else if(e.order==="ZXY"){let d=c*u,h=c*f,g=l*u,x=l*f;t[0]=d-x*a,t[4]=-o*f,t[8]=g+h*a,t[1]=h+g*a,t[5]=o*u,t[9]=x-d*a,t[2]=-o*l,t[6]=a,t[10]=o*c}else if(e.order==="ZYX"){let d=o*u,h=o*f,g=a*u,x=a*f;t[0]=c*u,t[4]=g*l-h,t[8]=d*l+x,t[1]=c*f,t[5]=x*l+d,t[9]=h*l-g,t[2]=-l,t[6]=a*c,t[10]=o*c}else if(e.order==="YZX"){let d=o*c,h=o*l,g=a*c,x=a*l;t[0]=c*u,t[4]=x-d*f,t[8]=g*f+h,t[1]=f,t[5]=o*u,t[9]=-a*u,t[2]=-l*u,t[6]=h*f+g,t[10]=d-x*f}else if(e.order==="XZY"){let d=o*c,h=o*l,g=a*c,x=a*l;t[0]=c*u,t[4]=-f,t[8]=l*u,t[1]=d*f+x,t[5]=o*u,t[9]=h*f-g,t[2]=g*f-h,t[6]=a*u,t[10]=x*f+d}return t[3]=0,t[7]=0,t[11]=0,t[12]=0,t[13]=0,t[14]=0,t[15]=1,this}makeRotationFromQuaternion(e){return this.compose(Pw,e,Lw)}lookAt(e,t,i){let r=this.elements;return Rn.subVectors(e,t),Rn.lengthSq()===0&&(Rn.z=1),Rn.normalize(),Cr.crossVectors(i,Rn),Cr.lengthSq()===0&&(Math.abs(i.z)===1?Rn.x+=1e-4:Rn.z+=1e-4,Rn.normalize(),Cr.crossVectors(i,Rn)),Cr.normalize(),ru.crossVectors(Rn,Cr),r[0]=Cr.x,r[4]=ru.x,r[8]=Rn.x,r[1]=Cr.y,r[5]=ru.y,r[9]=Rn.y,r[2]=Cr.z,r[6]=ru.z,r[10]=Rn.z,this}multiply(e){return this.multiplyMatrices(this,e)}premultiply(e){return this.multiplyMatrices(e,this)}multiplyMatrices(e,t){let i=e.elements,r=t.elements,s=this.elements,o=i[0],a=i[4],c=i[8],l=i[12],u=i[1],f=i[5],d=i[9],h=i[13],g=i[2],x=i[6],m=i[10],p=i[14],M=i[3],T=i[7],S=i[11],I=i[15],D=r[0],A=r[4],y=r[8],E=r[12],q=r[1],C=r[5],U=r[9],V=r[13],W=r[2],B=r[6],H=r[10],L=r[14],Q=r[3],Z=r[7],ue=r[11],ge=r[15];return s[0]=o*D+a*q+c*W+l*Q,s[4]=o*A+a*C+c*B+l*Z,s[8]=o*y+a*U+c*H+l*ue,s[12]=o*E+a*V+c*L+l*ge,s[1]=u*D+f*q+d*W+h*Q,s[5]=u*A+f*C+d*B+h*Z,s[9]=u*y+f*U+d*H+h*ue,s[13]=u*E+f*V+d*L+h*ge,s[2]=g*D+x*q+m*W+p*Q,s[6]=g*A+x*C+m*B+p*Z,s[10]=g*y+x*U+m*H+p*ue,s[14]=g*E+x*V+m*L+p*ge,s[3]=M*D+T*q+S*W+I*Q,s[7]=M*A+T*C+S*B+I*Z,s[11]=M*y+T*U+S*H+I*ue,s[15]=M*E+T*V+S*L+I*ge,this}multiplyScalar(e){let t=this.elements;return t[0]*=e,t[4]*=e,t[8]*=e,t[12]*=e,t[1]*=e,t[5]*=e,t[9]*=e,t[13]*=e,t[2]*=e,t[6]*=e,t[10]*=e,t[14]*=e,t[3]*=e,t[7]*=e,t[11]*=e,t[15]*=e,this}determinant(){let e=this.elements,t=e[0],i=e[4],r=e[8],s=e[12],o=e[1],a=e[5],c=e[9],l=e[13],u=e[2],f=e[6],d=e[10],h=e[14],g=e[3],x=e[7],m=e[11],p=e[15],M=c*h-l*d,T=a*h-l*f,S=a*d-c*f,I=o*h-l*u,D=o*d-c*u,A=o*f-a*u;return t*(x*M-m*T+p*S)-i*(g*M-m*I+p*D)+r*(g*T-x*I+p*A)-s*(g*S-x*D+m*A)}transpose(){let e=this.elements,t;return t=e[1],e[1]=e[4],e[4]=t,t=e[2],e[2]=e[8],e[8]=t,t=e[6],e[6]=e[9],e[9]=t,t=e[3],e[3]=e[12],e[12]=t,t=e[7],e[7]=e[13],e[13]=t,t=e[11],e[11]=e[14],e[14]=t,this}setPosition(e,t,i){let r=this.elements;return e.isVector3?(r[12]=e.x,r[13]=e.y,r[14]=e.z):(r[12]=e,r[13]=t,r[14]=i),this}invert(){let e=this.elements,t=e[0],i=e[1],r=e[2],s=e[3],o=e[4],a=e[5],c=e[6],l=e[7],u=e[8],f=e[9],d=e[10],h=e[11],g=e[12],x=e[13],m=e[14],p=e[15],M=t*a-i*o,T=t*c-r*o,S=t*l-s*o,I=i*c-r*a,D=i*l-s*a,A=r*l-s*c,y=u*x-f*g,E=u*m-d*g,q=u*p-h*g,C=f*m-d*x,U=f*p-h*x,V=d*p-h*m,W=M*V-T*U+S*C+I*q-D*E+A*y;if(W===0)return this.set(0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0);let B=1/W;return e[0]=(a*V-c*U+l*C)*B,e[1]=(r*U-i*V-s*C)*B,e[2]=(x*A-m*D+p*I)*B,e[3]=(d*D-f*A-h*I)*B,e[4]=(c*q-o*V-l*E)*B,e[5]=(t*V-r*q+s*E)*B,e[6]=(m*S-g*A-p*T)*B,e[7]=(u*A-d*S+h*T)*B,e[8]=(o*U-a*q+l*y)*B,e[9]=(i*q-t*U-s*y)*B,e[10]=(g*D-x*S+p*M)*B,e[11]=(f*S-u*D-h*M)*B,e[12]=(a*E-o*C-c*y)*B,e[13]=(t*C-i*E+r*y)*B,e[14]=(x*T-g*I-m*M)*B,e[15]=(u*I-f*T+d*M)*B,this}scale(e){let t=this.elements,i=e.x,r=e.y,s=e.z;return t[0]*=i,t[4]*=r,t[8]*=s,t[1]*=i,t[5]*=r,t[9]*=s,t[2]*=i,t[6]*=r,t[10]*=s,t[3]*=i,t[7]*=r,t[11]*=s,this}getMaxScaleOnAxis(){let e=this.elements,t=e[0]*e[0]+e[1]*e[1]+e[2]*e[2],i=e[4]*e[4]+e[5]*e[5]+e[6]*e[6],r=e[8]*e[8]+e[9]*e[9]+e[10]*e[10];return Math.sqrt(Math.max(t,i,r))}makeTranslation(e,t,i){return e.isVector3?this.set(1,0,0,e.x,0,1,0,e.y,0,0,1,e.z,0,0,0,1):this.set(1,0,0,e,0,1,0,t,0,0,1,i,0,0,0,1),this}makeRotationX(e){let t=Math.cos(e),i=Math.sin(e);return this.set(1,0,0,0,0,t,-i,0,0,i,t,0,0,0,0,1),this}makeRotationY(e){let t=Math.cos(e),i=Math.sin(e);return this.set(t,0,i,0,0,1,0,0,-i,0,t,0,0,0,0,1),this}makeRotationZ(e){let t=Math.cos(e),i=Math.sin(e);return this.set(t,-i,0,0,i,t,0,0,0,0,1,0,0,0,0,1),this}makeRotationAxis(e,t){let i=Math.cos(t),r=Math.sin(t),s=1-i,o=e.x,a=e.y,c=e.z,l=s*o,u=s*a;return this.set(l*o+i,l*a-r*c,l*c+r*a,0,l*a+r*c,u*a+i,u*c-r*o,0,l*c-r*a,u*c+r*o,s*c*c+i,0,0,0,0,1),this}makeScale(e,t,i){return this.set(e,0,0,0,0,t,0,0,0,0,i,0,0,0,0,1),this}makeShear(e,t,i,r,s,o){return this.set(1,i,s,0,e,1,o,0,t,r,1,0,0,0,0,1),this}compose(e,t,i){let r=this.elements,s=t._x,o=t._y,a=t._z,c=t._w,l=s+s,u=o+o,f=a+a,d=s*l,h=s*u,g=s*f,x=o*u,m=o*f,p=a*f,M=c*l,T=c*u,S=c*f,I=i.x,D=i.y,A=i.z;return r[0]=(1-(x+p))*I,r[1]=(h+S)*I,r[2]=(g-T)*I,r[3]=0,r[4]=(h-S)*D,r[5]=(1-(d+p))*D,r[6]=(m+M)*D,r[7]=0,r[8]=(g+T)*A,r[9]=(m-M)*A,r[10]=(1-(d+x))*A,r[11]=0,r[12]=e.x,r[13]=e.y,r[14]=e.z,r[15]=1,this}decompose(e,t,i){let r=this.elements;e.x=r[12],e.y=r[13],e.z=r[14];let s=this.determinant();if(s===0)return i.set(1,1,1),t.identity(),this;let o=po.set(r[0],r[1],r[2]).length(),a=po.set(r[4],r[5],r[6]).length(),c=po.set(r[8],r[9],r[10]).length();s<0&&(o=-o),ui.copy(this);let l=1/o,u=1/a,f=1/c;return ui.elements[0]*=l,ui.elements[1]*=l,ui.elements[2]*=l,ui.elements[4]*=u,ui.elements[5]*=u,ui.elements[6]*=u,ui.elements[8]*=f,ui.elements[9]*=f,ui.elements[10]*=f,t.setFromRotationMatrix(ui),i.x=o,i.y=a,i.z=c,this}makePerspective(e,t,i,r,s,o,a=hi,c=!1){let l=this.elements,u=2*s/(t-e),f=2*s/(i-r),d=(t+e)/(t-e),h=(i+r)/(i-r),g,x;if(c)g=s/(o-s),x=o*s/(o-s);else if(a===hi)g=-(o+s)/(o-s),x=-2*o*s/(o-s);else if(a===Ba)g=-o/(o-s),x=-o*s/(o-s);else throw new Error("THREE.Matrix4.makePerspective(): Invalid coordinate system: "+a);return l[0]=u,l[4]=0,l[8]=d,l[12]=0,l[1]=0,l[5]=f,l[9]=h,l[13]=0,l[2]=0,l[6]=0,l[10]=g,l[14]=x,l[3]=0,l[7]=0,l[11]=-1,l[15]=0,this}makeOrthographic(e,t,i,r,s,o,a=hi,c=!1){let l=this.elements,u=2/(t-e),f=2/(i-r),d=-(t+e)/(t-e),h=-(i+r)/(i-r),g,x;if(c)g=1/(o-s),x=o/(o-s);else if(a===hi)g=-2/(o-s),x=-(o+s)/(o-s);else if(a===Ba)g=-1/(o-s),x=-s/(o-s);else throw new Error("THREE.Matrix4.makeOrthographic(): Invalid coordinate system: "+a);return l[0]=u,l[4]=0,l[8]=0,l[12]=d,l[1]=0,l[5]=f,l[9]=0,l[13]=h,l[2]=0,l[6]=0,l[10]=g,l[14]=x,l[3]=0,l[7]=0,l[11]=0,l[15]=1,this}equals(e){let t=this.elements,i=e.elements;for(let r=0;r<16;r++)if(t[r]!==i[r])return!1;return!0}fromArray(e,t=0){for(let i=0;i<16;i++)this.elements[i]=e[i+t];return this}toArray(e=[],t=0){let i=this.elements;return e[t]=i[0],e[t+1]=i[1],e[t+2]=i[2],e[t+3]=i[3],e[t+4]=i[4],e[t+5]=i[5],e[t+6]=i[6],e[t+7]=i[7],e[t+8]=i[8],e[t+9]=i[9],e[t+10]=i[10],e[t+11]=i[11],e[t+12]=i[12],e[t+13]=i[13],e[t+14]=i[14],e[t+15]=i[15],e}},po=new k,ui=new Bt,Pw=new k(0,0,0),Lw=new k(1,1,1),Cr=new k,ru=new k,Rn=new k,tx=new Bt,nx=new Di,Ss=(()=>{class n{constructor(t=0,i=0,r=0,s=n.DEFAULT_ORDER){this.isEuler=!0,this._x=t,this._y=i,this._z=r,this._order=s}get x(){return this._x}set x(t){this._x=t,this._onChangeCallback()}get y(){return this._y}set y(t){this._y=t,this._onChangeCallback()}get z(){return this._z}set z(t){this._z=t,this._onChangeCallback()}get order(){return this._order}set order(t){this._order=t,this._onChangeCallback()}set(t,i,r,s=this._order){return this._x=t,this._y=i,this._z=r,this._order=s,this._onChangeCallback(),this}clone(){return new this.constructor(this._x,this._y,this._z,this._order)}copy(t){return this._x=t._x,this._y=t._y,this._z=t._z,this._order=t._order,this._onChangeCallback(),this}setFromRotationMatrix(t,i=this._order,r=!0){let s=t.elements,o=s[0],a=s[4],c=s[8],l=s[1],u=s[5],f=s[9],d=s[2],h=s[6],g=s[10];switch(i){case"XYZ":this._y=Math.asin(Ke(c,-1,1)),Math.abs(c)<.9999999?(this._x=Math.atan2(-f,g),this._z=Math.atan2(-a,o)):(this._x=Math.atan2(h,u),this._z=0);break;case"YXZ":this._x=Math.asin(-Ke(f,-1,1)),Math.abs(f)<.9999999?(this._y=Math.atan2(c,g),this._z=Math.atan2(l,u)):(this._y=Math.atan2(-d,o),this._z=0);break;case"ZXY":this._x=Math.asin(Ke(h,-1,1)),Math.abs(h)<.9999999?(this._y=Math.atan2(-d,g),this._z=Math.atan2(-a,u)):(this._y=0,this._z=Math.atan2(l,o));break;case"ZYX":this._y=Math.asin(-Ke(d,-1,1)),Math.abs(d)<.9999999?(this._x=Math.atan2(h,g),this._z=Math.atan2(l,o)):(this._x=0,this._z=Math.atan2(-a,u));break;case"YZX":this._z=Math.asin(Ke(l,-1,1)),Math.abs(l)<.9999999?(this._x=Math.atan2(-f,u),this._y=Math.atan2(-d,o)):(this._x=0,this._y=Math.atan2(c,g));break;case"XZY":this._z=Math.asin(-Ke(a,-1,1)),Math.abs(a)<.9999999?(this._x=Math.atan2(h,u),this._y=Math.atan2(c,o)):(this._x=Math.atan2(-f,g),this._y=0);break;default:Le("Euler: .setFromRotationMatrix() encountered an unknown order: "+i)}return this._order=i,r===!0&&this._onChangeCallback(),this}setFromQuaternion(t,i,r){return tx.makeRotationFromQuaternion(t),this.setFromRotationMatrix(tx,i,r)}setFromVector3(t,i=this._order){return this.set(t.x,t.y,t.z,i)}reorder(t){return nx.setFromEuler(this),this.setFromQuaternion(nx,t)}equals(t){return t._x===this._x&&t._y===this._y&&t._z===this._z&&t._order===this._order}fromArray(t){return this._x=t[0],this._y=t[1],this._z=t[2],t[3]!==void 0&&(this._order=t[3]),this._onChangeCallback(),this}toArray(t=[],i=0){return t[i]=this._x,t[i+1]=this._y,t[i+2]=this._z,t[i+3]=this._order,t}_onChange(t){return this._onChangeCallback=t,this}_onChangeCallback(){}*[Symbol.iterator](){yield this._x,yield this._y,yield this._z,yield this._order}}return n.DEFAULT_ORDER="XYZ",n})(),Ga=class{constructor(){this.mask=1}set(e){this.mask=(1<<e|0)>>>0}enable(e){this.mask|=1<<e|0}enableAll(){this.mask=-1}toggle(e){this.mask^=1<<e|0}disable(e){this.mask&=~(1<<e|0)}disableAll(){this.mask=0}test(e){return(this.mask&e.mask)!==0}isEnabled(e){return(this.mask&(1<<e|0))!==0}},Fw=0,ix=new k,mo=new Di,Ji=new Bt,su=new k,Pa=new k,Ow=new k,kw=new Di,rx=new k(1,0,0),sx=new k(0,1,0),ox=new k(0,0,1),ax={type:"added"},Uw={type:"removed"},go={type:"childadded",child:null},Em={type:"childremoved",child:null},cr=(()=>{class n extends sr{constructor(){super(),this.isObject3D=!0,Object.defineProperty(this,"id",{value:Fw++}),this.uuid=dc(),this.name="",this.type="Object3D",this.parent=null,this.children=[],this.up=n.DEFAULT_UP.clone();let t=new k,i=new Ss,r=new Di,s=new k(1,1,1);function o(){r.setFromEuler(i,!1)}function a(){i.setFromQuaternion(r,void 0,!1)}i._onChange(o),r._onChange(a),Object.defineProperties(this,{position:{configurable:!0,enumerable:!0,value:t},rotation:{configurable:!0,enumerable:!0,value:i},quaternion:{configurable:!0,enumerable:!0,value:r},scale:{configurable:!0,enumerable:!0,value:s},modelViewMatrix:{value:new Bt},normalMatrix:{value:new Be}}),this.matrix=new Bt,this.matrixWorld=new Bt,this.matrixAutoUpdate=n.DEFAULT_MATRIX_AUTO_UPDATE,this.matrixWorldAutoUpdate=n.DEFAULT_MATRIX_WORLD_AUTO_UPDATE,this.matrixWorldNeedsUpdate=!1,this.layers=new Ga,this.visible=!0,this.castShadow=!1,this.receiveShadow=!1,this.frustumCulled=!0,this.renderOrder=0,this.animations=[],this.customDepthMaterial=void 0,this.customDistanceMaterial=void 0,this.static=!1,this.userData={},this.pivot=null}onBeforeShadow(){}onAfterShadow(){}onBeforeRender(){}onAfterRender(){}applyMatrix4(t){this.matrixAutoUpdate&&this.updateMatrix(),this.matrix.premultiply(t),this.matrix.decompose(this.position,this.quaternion,this.scale)}applyQuaternion(t){return this.quaternion.premultiply(t),this}setRotationFromAxisAngle(t,i){this.quaternion.setFromAxisAngle(t,i)}setRotationFromEuler(t){this.quaternion.setFromEuler(t,!0)}setRotationFromMatrix(t){this.quaternion.setFromRotationMatrix(t)}setRotationFromQuaternion(t){this.quaternion.copy(t)}rotateOnAxis(t,i){return mo.setFromAxisAngle(t,i),this.quaternion.multiply(mo),this}rotateOnWorldAxis(t,i){return mo.setFromAxisAngle(t,i),this.quaternion.premultiply(mo),this}rotateX(t){return this.rotateOnAxis(rx,t)}rotateY(t){return this.rotateOnAxis(sx,t)}rotateZ(t){return this.rotateOnAxis(ox,t)}translateOnAxis(t,i){return ix.copy(t).applyQuaternion(this.quaternion),this.position.add(ix.multiplyScalar(i)),this}translateX(t){return this.translateOnAxis(rx,t)}translateY(t){return this.translateOnAxis(sx,t)}translateZ(t){return this.translateOnAxis(ox,t)}localToWorld(t){return this.updateWorldMatrix(!0,!1),t.applyMatrix4(this.matrixWorld)}worldToLocal(t){return this.updateWorldMatrix(!0,!1),t.applyMatrix4(Ji.copy(this.matrixWorld).invert())}lookAt(t,i,r){t.isVector3?su.copy(t):su.set(t,i,r);let s=this.parent;this.updateWorldMatrix(!0,!1),Pa.setFromMatrixPosition(this.matrixWorld),this.isCamera||this.isLight?Ji.lookAt(Pa,su,this.up):Ji.lookAt(su,Pa,this.up),this.quaternion.setFromRotationMatrix(Ji),s&&(Ji.extractRotation(s.matrixWorld),mo.setFromRotationMatrix(Ji),this.quaternion.premultiply(mo.invert()))}add(t){if(arguments.length>1){for(let i=0;i<arguments.length;i++)this.add(arguments[i]);return this}return t===this?(Ne("Object3D.add: object can't be added as a child of itself.",t),this):(t&&t.isObject3D?(t.removeFromParent(),t.parent=this,this.children.push(t),t.dispatchEvent(ax),go.child=t,this.dispatchEvent(go),go.child=null):Ne("Object3D.add: object not an instance of THREE.Object3D.",t),this)}remove(t){if(arguments.length>1){for(let r=0;r<arguments.length;r++)this.remove(arguments[r]);return this}let i=this.children.indexOf(t);return i!==-1&&(t.parent=null,this.children.splice(i,1),t.dispatchEvent(Uw),Em.child=t,this.dispatchEvent(Em),Em.child=null),this}removeFromParent(){let t=this.parent;return t!==null&&t.remove(this),this}clear(){return this.remove(...this.children)}attach(t){return this.updateWorldMatrix(!0,!1),Ji.copy(this.matrixWorld).invert(),t.parent!==null&&(t.parent.updateWorldMatrix(!0,!1),Ji.multiply(t.parent.matrixWorld)),t.applyMatrix4(Ji),t.removeFromParent(),t.parent=this,this.children.push(t),t.updateWorldMatrix(!1,!0),t.dispatchEvent(ax),go.child=t,this.dispatchEvent(go),go.child=null,this}getObjectById(t){return this.getObjectByProperty("id",t)}getObjectByName(t){return this.getObjectByProperty("name",t)}getObjectByProperty(t,i){if(this[t]===i)return this;for(let r=0,s=this.children.length;r<s;r++){let a=this.children[r].getObjectByProperty(t,i);if(a!==void 0)return a}}getObjectsByProperty(t,i,r=[]){this[t]===i&&r.push(this);let s=this.children;for(let o=0,a=s.length;o<a;o++)s[o].getObjectsByProperty(t,i,r);return r}getWorldPosition(t){return this.updateWorldMatrix(!0,!1),t.setFromMatrixPosition(this.matrixWorld)}getWorldQuaternion(t){return this.updateWorldMatrix(!0,!1),this.matrixWorld.decompose(Pa,t,Ow),t}getWorldScale(t){return this.updateWorldMatrix(!0,!1),this.matrixWorld.decompose(Pa,kw,t),t}getWorldDirection(t){this.updateWorldMatrix(!0,!1);let i=this.matrixWorld.elements;return t.set(i[8],i[9],i[10]).normalize()}raycast(){}traverse(t){t(this);let i=this.children;for(let r=0,s=i.length;r<s;r++)i[r].traverse(t)}traverseVisible(t){if(this.visible===!1)return;t(this);let i=this.children;for(let r=0,s=i.length;r<s;r++)i[r].traverseVisible(t)}traverseAncestors(t){let i=this.parent;i!==null&&(t(i),i.traverseAncestors(t))}updateMatrix(){this.matrix.compose(this.position,this.quaternion,this.scale);let t=this.pivot;if(t!==null){let i=t.x,r=t.y,s=t.z,o=this.matrix.elements;o[12]+=i-o[0]*i-o[4]*r-o[8]*s,o[13]+=r-o[1]*i-o[5]*r-o[9]*s,o[14]+=s-o[2]*i-o[6]*r-o[10]*s}this.matrixWorldNeedsUpdate=!0}updateMatrixWorld(t){this.matrixAutoUpdate&&this.updateMatrix(),(this.matrixWorldNeedsUpdate||t)&&(this.matrixWorldAutoUpdate===!0&&(this.parent===null?this.matrixWorld.copy(this.matrix):this.matrixWorld.multiplyMatrices(this.parent.matrixWorld,this.matrix)),this.matrixWorldNeedsUpdate=!1,t=!0);let i=this.children;for(let r=0,s=i.length;r<s;r++)i[r].updateMatrixWorld(t)}updateWorldMatrix(t,i){let r=this.parent;if(t===!0&&r!==null&&r.updateWorldMatrix(!0,!1),this.matrixAutoUpdate&&this.updateMatrix(),this.matrixWorldAutoUpdate===!0&&(this.parent===null?this.matrixWorld.copy(this.matrix):this.matrixWorld.multiplyMatrices(this.parent.matrixWorld,this.matrix)),i===!0){let s=this.children;for(let o=0,a=s.length;o<a;o++)s[o].updateWorldMatrix(!1,!0)}}toJSON(t){let i=t===void 0||typeof t=="string",r={};i&&(t={geometries:{},materials:{},textures:{},images:{},shapes:{},skeletons:{},animations:{},nodes:{}},r.metadata={version:4.7,type:"Object",generator:"Object3D.toJSON"});let s={};s.uuid=this.uuid,s.type=this.type,this.name!==""&&(s.name=this.name),this.castShadow===!0&&(s.castShadow=!0),this.receiveShadow===!0&&(s.receiveShadow=!0),this.visible===!1&&(s.visible=!1),this.frustumCulled===!1&&(s.frustumCulled=!1),this.renderOrder!==0&&(s.renderOrder=this.renderOrder),this.static!==!1&&(s.static=this.static),Object.keys(this.userData).length>0&&(s.userData=this.userData),s.layers=this.layers.mask,s.matrix=this.matrix.toArray(),s.up=this.up.toArray(),this.pivot!==null&&(s.pivot=this.pivot.toArray()),this.matrixAutoUpdate===!1&&(s.matrixAutoUpdate=!1),this.morphTargetDictionary!==void 0&&(s.morphTargetDictionary=Object.assign({},this.morphTargetDictionary)),this.morphTargetInfluences!==void 0&&(s.morphTargetInfluences=this.morphTargetInfluences.slice()),this.isInstancedMesh&&(s.type="InstancedMesh",s.count=this.count,s.instanceMatrix=this.instanceMatrix.toJSON(),this.instanceColor!==null&&(s.instanceColor=this.instanceColor.toJSON())),this.isBatchedMesh&&(s.type="BatchedMesh",s.perObjectFrustumCulled=this.perObjectFrustumCulled,s.sortObjects=this.sortObjects,s.drawRanges=this._drawRanges,s.reservedRanges=this._reservedRanges,s.geometryInfo=this._geometryInfo.map(c=>Wt(It({},c),{boundingBox:c.boundingBox?c.boundingBox.toJSON():void 0,boundingSphere:c.boundingSphere?c.boundingSphere.toJSON():void 0})),s.instanceInfo=this._instanceInfo.map(c=>It({},c)),s.availableInstanceIds=this._availableInstanceIds.slice(),s.availableGeometryIds=this._availableGeometryIds.slice(),s.nextIndexStart=this._nextIndexStart,s.nextVertexStart=this._nextVertexStart,s.geometryCount=this._geometryCount,s.maxInstanceCount=this._maxInstanceCount,s.maxVertexCount=this._maxVertexCount,s.maxIndexCount=this._maxIndexCount,s.geometryInitialized=this._geometryInitialized,s.matricesTexture=this._matricesTexture.toJSON(t),s.indirectTexture=this._indirectTexture.toJSON(t),this._colorsTexture!==null&&(s.colorsTexture=this._colorsTexture.toJSON(t)),this.boundingSphere!==null&&(s.boundingSphere=this.boundingSphere.toJSON()),this.boundingBox!==null&&(s.boundingBox=this.boundingBox.toJSON()));function o(c,l){return c[l.uuid]===void 0&&(c[l.uuid]=l.toJSON(t)),l.uuid}if(this.isScene)this.background&&(this.background.isColor?s.background=this.background.toJSON():this.background.isTexture&&(s.background=this.background.toJSON(t).uuid)),this.environment&&this.environment.isTexture&&this.environment.isRenderTargetTexture!==!0&&(s.environment=this.environment.toJSON(t).uuid);else if(this.isMesh||this.isLine||this.isPoints){s.geometry=o(t.geometries,this.geometry);let c=this.geometry.parameters;if(c!==void 0&&c.shapes!==void 0){let l=c.shapes;if(Array.isArray(l))for(let u=0,f=l.length;u<f;u++){let d=l[u];o(t.shapes,d)}else o(t.shapes,l)}}if(this.isSkinnedMesh&&(s.bindMode=this.bindMode,s.bindMatrix=this.bindMatrix.toArray(),this.skeleton!==void 0&&(o(t.skeletons,this.skeleton),s.skeleton=this.skeleton.uuid)),this.material!==void 0)if(Array.isArray(this.material)){let c=[];for(let l=0,u=this.material.length;l<u;l++)c.push(o(t.materials,this.material[l]));s.material=c}else s.material=o(t.materials,this.material);if(this.children.length>0){s.children=[];for(let c=0;c<this.children.length;c++)s.children.push(this.children[c].toJSON(t).object)}if(this.animations.length>0){s.animations=[];for(let c=0;c<this.animations.length;c++){let l=this.animations[c];s.animations.push(o(t.animations,l))}}if(i){let c=a(t.geometries),l=a(t.materials),u=a(t.textures),f=a(t.images),d=a(t.shapes),h=a(t.skeletons),g=a(t.animations),x=a(t.nodes);c.length>0&&(r.geometries=c),l.length>0&&(r.materials=l),u.length>0&&(r.textures=u),f.length>0&&(r.images=f),d.length>0&&(r.shapes=d),h.length>0&&(r.skeletons=h),g.length>0&&(r.animations=g),x.length>0&&(r.nodes=x)}return r.object=s,r;function a(c){let l=[];for(let u in c){let f=c[u];delete f.metadata,l.push(f)}return l}}clone(t){return new this.constructor().copy(this,t)}copy(t,i=!0){if(this.name=t.name,this.up.copy(t.up),this.position.copy(t.position),this.rotation.order=t.rotation.order,this.quaternion.copy(t.quaternion),this.scale.copy(t.scale),t.pivot!==null&&(this.pivot=t.pivot.clone()),this.matrix.copy(t.matrix),this.matrixWorld.copy(t.matrixWorld),this.matrixAutoUpdate=t.matrixAutoUpdate,this.matrixWorldAutoUpdate=t.matrixWorldAutoUpdate,this.matrixWorldNeedsUpdate=t.matrixWorldNeedsUpdate,this.layers.mask=t.layers.mask,this.visible=t.visible,this.castShadow=t.castShadow,this.receiveShadow=t.receiveShadow,this.frustumCulled=t.frustumCulled,this.renderOrder=t.renderOrder,this.static=t.static,this.animations=t.animations.slice(),this.userData=JSON.parse(JSON.stringify(t.userData)),i===!0)for(let r=0;r<t.children.length;r++){let s=t.children[r];this.add(s.clone())}return this}}return n.DEFAULT_UP=new k(0,1,0),n.DEFAULT_MATRIX_AUTO_UPDATE=!0,n.DEFAULT_MATRIX_WORLD_AUTO_UPDATE=!0,n})(),Ms=class extends cr{constructor(){super(),this.isGroup=!0,this.type="Group"}},Bw={type:"move"},Do=class{constructor(){this._targetRay=null,this._grip=null,this._hand=null}getHandSpace(){return this._hand===null&&(this._hand=new Ms,this._hand.matrixAutoUpdate=!1,this._hand.visible=!1,this._hand.joints={},this._hand.inputState={pinching:!1}),this._hand}getTargetRaySpace(){return this._targetRay===null&&(this._targetRay=new Ms,this._targetRay.matrixAutoUpdate=!1,this._targetRay.visible=!1,this._targetRay.hasLinearVelocity=!1,this._targetRay.linearVelocity=new k,this._targetRay.hasAngularVelocity=!1,this._targetRay.angularVelocity=new k),this._targetRay}getGripSpace(){return this._grip===null&&(this._grip=new Ms,this._grip.matrixAutoUpdate=!1,this._grip.visible=!1,this._grip.hasLinearVelocity=!1,this._grip.linearVelocity=new k,this._grip.hasAngularVelocity=!1,this._grip.angularVelocity=new k),this._grip}dispatchEvent(e){return this._targetRay!==null&&this._targetRay.dispatchEvent(e),this._grip!==null&&this._grip.dispatchEvent(e),this._hand!==null&&this._hand.dispatchEvent(e),this}connect(e){if(e&&e.hand){let t=this._hand;if(t)for(let i of e.hand.values())this._getHandJoint(t,i)}return this.dispatchEvent({type:"connected",data:e}),this}disconnect(e){return this.dispatchEvent({type:"disconnected",data:e}),this._targetRay!==null&&(this._targetRay.visible=!1),this._grip!==null&&(this._grip.visible=!1),this._hand!==null&&(this._hand.visible=!1),this}update(e,t,i){let r=null,s=null,o=null,a=this._targetRay,c=this._grip,l=this._hand;if(e&&t.session.visibilityState!=="visible-blurred"){if(l&&e.hand){o=!0;for(let x of e.hand.values()){let m=t.getJointPose(x,i),p=this._getHandJoint(l,x);m!==null&&(p.matrix.fromArray(m.transform.matrix),p.matrix.decompose(p.position,p.rotation,p.scale),p.matrixWorldNeedsUpdate=!0,p.jointRadius=m.radius),p.visible=m!==null}let u=l.joints["index-finger-tip"],f=l.joints["thumb-tip"],d=u.position.distanceTo(f.position),h=.02,g=.005;l.inputState.pinching&&d>h+g?(l.inputState.pinching=!1,this.dispatchEvent({type:"pinchend",handedness:e.handedness,target:this})):!l.inputState.pinching&&d<=h-g&&(l.inputState.pinching=!0,this.dispatchEvent({type:"pinchstart",handedness:e.handedness,target:this}))}else c!==null&&e.gripSpace&&(s=t.getPose(e.gripSpace,i),s!==null&&(c.matrix.fromArray(s.transform.matrix),c.matrix.decompose(c.position,c.rotation,c.scale),c.matrixWorldNeedsUpdate=!0,s.linearVelocity?(c.hasLinearVelocity=!0,c.linearVelocity.copy(s.linearVelocity)):c.hasLinearVelocity=!1,s.angularVelocity?(c.hasAngularVelocity=!0,c.angularVelocity.copy(s.angularVelocity)):c.hasAngularVelocity=!1));a!==null&&(r=t.getPose(e.targetRaySpace,i),r===null&&s!==null&&(r=s),r!==null&&(a.matrix.fromArray(r.transform.matrix),a.matrix.decompose(a.position,a.rotation,a.scale),a.matrixWorldNeedsUpdate=!0,r.linearVelocity?(a.hasLinearVelocity=!0,a.linearVelocity.copy(r.linearVelocity)):a.hasLinearVelocity=!1,r.angularVelocity?(a.hasAngularVelocity=!0,a.angularVelocity.copy(r.angularVelocity)):a.hasAngularVelocity=!1,this.dispatchEvent(Bw)))}return a!==null&&(a.visible=r!==null),c!==null&&(c.visible=s!==null),l!==null&&(l.visible=o!==null),this}_getHandJoint(e,t){if(e.joints[t.jointName]===void 0){let i=new Ms;i.matrixAutoUpdate=!1,i.visible=!1,e.joints[t.jointName]=i,e.add(i)}return e.joints[t.jointName]}},Qx={aliceblue:15792383,antiquewhite:16444375,aqua:65535,aquamarine:8388564,azure:15794175,beige:16119260,bisque:16770244,black:0,blanchedalmond:16772045,blue:255,blueviolet:9055202,brown:10824234,burlywood:14596231,cadetblue:6266528,chartreuse:8388352,chocolate:13789470,coral:16744272,cornflowerblue:6591981,cornsilk:16775388,crimson:14423100,cyan:65535,darkblue:139,darkcyan:35723,darkgoldenrod:12092939,darkgray:11119017,darkgreen:25600,darkgrey:11119017,darkkhaki:12433259,darkmagenta:9109643,darkolivegreen:5597999,darkorange:16747520,darkorchid:10040012,darkred:9109504,darksalmon:15308410,darkseagreen:9419919,darkslateblue:4734347,darkslategray:3100495,darkslategrey:3100495,darkturquoise:52945,darkviolet:9699539,deeppink:16716947,deepskyblue:49151,dimgray:6908265,dimgrey:6908265,dodgerblue:2003199,firebrick:11674146,floralwhite:16775920,forestgreen:2263842,fuchsia:16711935,gainsboro:14474460,ghostwhite:16316671,gold:16766720,goldenrod:14329120,gray:8421504,green:32768,greenyellow:11403055,grey:8421504,honeydew:15794160,hotpink:16738740,indianred:13458524,indigo:4915330,ivory:16777200,khaki:15787660,lavender:15132410,lavenderblush:16773365,lawngreen:8190976,lemonchiffon:16775885,lightblue:11393254,lightcoral:15761536,lightcyan:14745599,lightgoldenrodyellow:16448210,lightgray:13882323,lightgreen:9498256,lightgrey:13882323,lightpink:16758465,lightsalmon:16752762,lightseagreen:2142890,lightskyblue:8900346,lightslategray:7833753,lightslategrey:7833753,lightsteelblue:11584734,lightyellow:16777184,lime:65280,limegreen:3329330,linen:16445670,magenta:16711935,maroon:8388608,mediumaquamarine:6737322,mediumblue:205,mediumorchid:12211667,mediumpurple:9662683,mediumseagreen:3978097,mediumslateblue:8087790,mediumspringgreen:64154,mediumturquoise:4772300,mediumvioletred:13047173,midnightblue:1644912,mintcream:16121850,mistyrose:16770273,moccasin:16770229,navajowhite:16768685,navy:128,oldlace:16643558,olive:8421376,olivedrab:7048739,orange:16753920,orangered:16729344,orchid:14315734,palegoldenrod:15657130,palegreen:10025880,paleturquoise:11529966,palevioletred:14381203,papayawhip:16773077,peachpuff:16767673,peru:13468991,pink:16761035,plum:14524637,powderblue:11591910,purple:8388736,rebeccapurple:6697881,red:16711680,rosybrown:12357519,royalblue:4286945,saddlebrown:9127187,salmon:16416882,sandybrown:16032864,seagreen:3050327,seashell:16774638,sienna:10506797,silver:12632256,skyblue:8900331,slateblue:6970061,slategray:7372944,slategrey:7372944,snow:16775930,springgreen:65407,steelblue:4620980,tan:13808780,teal:32896,thistle:14204888,tomato:16737095,turquoise:4251856,violet:15631086,wheat:16113331,white:16777215,whitesmoke:16119285,yellow:16776960,yellowgreen:10145074},wr={h:0,s:0,l:0},ou={h:0,s:0,l:0};function Sm(n,e,t){return t<0&&(t+=1),t>1&&(t-=1),t<1/6?n+(e-n)*6*t:t<1/2?e:t<2/3?n+(e-n)*6*(2/3-t):n}var Xe=class{constructor(e,t,i){return this.isColor=!0,this.r=1,this.g=1,this.b=1,this.set(e,t,i)}set(e,t,i){if(t===void 0&&i===void 0){let r=e;r&&r.isColor?this.copy(r):typeof r=="number"?this.setHex(r):typeof r=="string"&&this.setStyle(r)}else this.setRGB(e,t,i);return this}setScalar(e){return this.r=e,this.g=e,this.b=e,this}setHex(e,t=Pn){return e=Math.floor(e),this.r=(e>>16&255)/255,this.g=(e>>8&255)/255,this.b=(e&255)/255,rt.colorSpaceToWorking(this,t),this}setRGB(e,t,i,r=rt.workingColorSpace){return this.r=e,this.g=t,this.b=i,rt.colorSpaceToWorking(this,r),this}setHSL(e,t,i,r=rt.workingColorSpace){if(e=Iw(e,1),t=Ke(t,0,1),i=Ke(i,0,1),t===0)this.r=this.g=this.b=i;else{let s=i<=.5?i*(1+t):i+t-i*t,o=2*i-s;this.r=Sm(o,s,e+1/3),this.g=Sm(o,s,e),this.b=Sm(o,s,e-1/3)}return rt.colorSpaceToWorking(this,r),this}setStyle(e,t=Pn){function i(s){s!==void 0&&parseFloat(s)<1&&Le("Color: Alpha component of "+e+" will be ignored.")}let r;if(r=/^(\w+)\(([^\)]*)\)/.exec(e)){let s,o=r[1],a=r[2];switch(o){case"rgb":case"rgba":if(s=/^\s*(\d+)\s*,\s*(\d+)\s*,\s*(\d+)\s*(?:,\s*(\d*\.?\d+)\s*)?$/.exec(a))return i(s[4]),this.setRGB(Math.min(255,parseInt(s[1],10))/255,Math.min(255,parseInt(s[2],10))/255,Math.min(255,parseInt(s[3],10))/255,t);if(s=/^\s*(\d+)\%\s*,\s*(\d+)\%\s*,\s*(\d+)\%\s*(?:,\s*(\d*\.?\d+)\s*)?$/.exec(a))return i(s[4]),this.setRGB(Math.min(100,parseInt(s[1],10))/100,Math.min(100,parseInt(s[2],10))/100,Math.min(100,parseInt(s[3],10))/100,t);break;case"hsl":case"hsla":if(s=/^\s*(\d*\.?\d+)\s*,\s*(\d*\.?\d+)\%\s*,\s*(\d*\.?\d+)\%\s*(?:,\s*(\d*\.?\d+)\s*)?$/.exec(a))return i(s[4]),this.setHSL(parseFloat(s[1])/360,parseFloat(s[2])/100,parseFloat(s[3])/100,t);break;default:Le("Color: Unknown color model "+e)}}else if(r=/^\#([A-Fa-f\d]+)$/.exec(e)){let s=r[1],o=s.length;if(o===3)return this.setRGB(parseInt(s.charAt(0),16)/15,parseInt(s.charAt(1),16)/15,parseInt(s.charAt(2),16)/15,t);if(o===6)return this.setHex(parseInt(s,16),t);Le("Color: Invalid hex color "+e)}else if(e&&e.length>0)return this.setColorName(e,t);return this}setColorName(e,t=Pn){let i=Qx[e.toLowerCase()];return i!==void 0?this.setHex(i,t):Le("Color: Unknown color "+e),this}clone(){return new this.constructor(this.r,this.g,this.b)}copy(e){return this.r=e.r,this.g=e.g,this.b=e.b,this}copySRGBToLinear(e){return this.r=nr(e.r),this.g=nr(e.g),this.b=nr(e.b),this}copyLinearToSRGB(e){return this.r=To(e.r),this.g=To(e.g),this.b=To(e.b),this}convertSRGBToLinear(){return this.copySRGBToLinear(this),this}convertLinearToSRGB(){return this.copyLinearToSRGB(this),this}getHex(e=Pn){return rt.workingToColorSpace(fn.copy(this),e),Math.round(Ke(fn.r*255,0,255))*65536+Math.round(Ke(fn.g*255,0,255))*256+Math.round(Ke(fn.b*255,0,255))}getHexString(e=Pn){return("000000"+this.getHex(e).toString(16)).slice(-6)}getHSL(e,t=rt.workingColorSpace){rt.workingToColorSpace(fn.copy(this),t);let i=fn.r,r=fn.g,s=fn.b,o=Math.max(i,r,s),a=Math.min(i,r,s),c,l,u=(a+o)/2;if(a===o)c=0,l=0;else{let f=o-a;switch(l=u<=.5?f/(o+a):f/(2-o-a),o){case i:c=(r-s)/f+(r<s?6:0);break;case r:c=(s-i)/f+2;break;case s:c=(i-r)/f+4;break}c/=6}return e.h=c,e.s=l,e.l=u,e}getRGB(e,t=rt.workingColorSpace){return rt.workingToColorSpace(fn.copy(this),t),e.r=fn.r,e.g=fn.g,e.b=fn.b,e}getStyle(e=Pn){rt.workingToColorSpace(fn.copy(this),e);let t=fn.r,i=fn.g,r=fn.b;return e!==Pn?`color(${e} ${t.toFixed(3)} ${i.toFixed(3)} ${r.toFixed(3)})`:`rgb(${Math.round(t*255)},${Math.round(i*255)},${Math.round(r*255)})`}offsetHSL(e,t,i){return this.getHSL(wr),this.setHSL(wr.h+e,wr.s+t,wr.l+i)}add(e){return this.r+=e.r,this.g+=e.g,this.b+=e.b,this}addColors(e,t){return this.r=e.r+t.r,this.g=e.g+t.g,this.b=e.b+t.b,this}addScalar(e){return this.r+=e,this.g+=e,this.b+=e,this}sub(e){return this.r=Math.max(0,this.r-e.r),this.g=Math.max(0,this.g-e.g),this.b=Math.max(0,this.b-e.b),this}multiply(e){return this.r*=e.r,this.g*=e.g,this.b*=e.b,this}multiplyScalar(e){return this.r*=e,this.g*=e,this.b*=e,this}lerp(e,t){return this.r+=(e.r-this.r)*t,this.g+=(e.g-this.g)*t,this.b+=(e.b-this.b)*t,this}lerpColors(e,t,i){return this.r=e.r+(t.r-e.r)*i,this.g=e.g+(t.g-e.g)*i,this.b=e.b+(t.b-e.b)*i,this}lerpHSL(e,t){this.getHSL(wr),e.getHSL(ou);let i=ym(wr.h,ou.h,t),r=ym(wr.s,ou.s,t),s=ym(wr.l,ou.l,t);return this.setHSL(i,r,s),this}setFromVector3(e){return this.r=e.x,this.g=e.y,this.b=e.z,this}applyMatrix3(e){let t=this.r,i=this.g,r=this.b,s=e.elements;return this.r=s[0]*t+s[3]*i+s[6]*r,this.g=s[1]*t+s[4]*i+s[7]*r,this.b=s[2]*t+s[5]*i+s[8]*r,this}equals(e){return e.r===this.r&&e.g===this.g&&e.b===this.b}fromArray(e,t=0){return this.r=e[t],this.g=e[t+1],this.b=e[t+2],this}toArray(e=[],t=0){return e[t]=this.r,e[t+1]=this.g,e[t+2]=this.b,e}fromBufferAttribute(e,t){return this.r=e.getX(t),this.g=e.getY(t),this.b=e.getZ(t),this}toJSON(){return this.getHex()}*[Symbol.iterator](){yield this.r,yield this.g,yield this.b}},fn=new Xe;Xe.NAMES=Qx;var Wa=class extends cr{constructor(){super(),this.isScene=!0,this.type="Scene",this.background=null,this.environment=null,this.fog=null,this.backgroundBlurriness=0,this.backgroundIntensity=1,this.backgroundRotation=new Ss,this.environmentIntensity=1,this.environmentRotation=new Ss,this.overrideMaterial=null,typeof __THREE_DEVTOOLS__<"u"&&__THREE_DEVTOOLS__.dispatchEvent(new CustomEvent("observe",{detail:this}))}copy(e,t){return super.copy(e,t),e.background!==null&&(this.background=e.background.clone()),e.environment!==null&&(this.environment=e.environment.clone()),e.fog!==null&&(this.fog=e.fog.clone()),this.backgroundBlurriness=e.backgroundBlurriness,this.backgroundIntensity=e.backgroundIntensity,this.backgroundRotation.copy(e.backgroundRotation),this.environmentIntensity=e.environmentIntensity,this.environmentRotation.copy(e.environmentRotation),e.overrideMaterial!==null&&(this.overrideMaterial=e.overrideMaterial.clone()),this.matrixAutoUpdate=e.matrixAutoUpdate,this}toJSON(e){let t=super.toJSON(e);return this.fog!==null&&(t.object.fog=this.fog.toJSON()),this.backgroundBlurriness>0&&(t.object.backgroundBlurriness=this.backgroundBlurriness),this.backgroundIntensity!==1&&(t.object.backgroundIntensity=this.backgroundIntensity),t.object.backgroundRotation=this.backgroundRotation.toArray(),this.environmentIntensity!==1&&(t.object.environmentIntensity=this.environmentIntensity),t.object.environmentRotation=this.environmentRotation.toArray(),t}},di=new k,Ki=new k,Tm=new k,Qi=new k,vo=new k,yo=new k,cx=new k,Cm=new k,wm=new k,Dm=new k,Im=new Lt,Am=new Lt,Rm=new Lt,Nr=class n{constructor(e=new k,t=new k,i=new k){this.a=e,this.b=t,this.c=i}static getNormal(e,t,i,r){r.subVectors(i,t),di.subVectors(e,t),r.cross(di);let s=r.lengthSq();return s>0?r.multiplyScalar(1/Math.sqrt(s)):r.set(0,0,0)}static getBarycoord(e,t,i,r,s){di.subVectors(r,t),Ki.subVectors(i,t),Tm.subVectors(e,t);let o=di.dot(di),a=di.dot(Ki),c=di.dot(Tm),l=Ki.dot(Ki),u=Ki.dot(Tm),f=o*l-a*a;if(f===0)return s.set(0,0,0),null;let d=1/f,h=(l*c-a*u)*d,g=(o*u-a*c)*d;return s.set(1-h-g,g,h)}static containsPoint(e,t,i,r){return this.getBarycoord(e,t,i,r,Qi)===null?!1:Qi.x>=0&&Qi.y>=0&&Qi.x+Qi.y<=1}static getInterpolation(e,t,i,r,s,o,a,c){return this.getBarycoord(e,t,i,r,Qi)===null?(c.x=0,c.y=0,"z"in c&&(c.z=0),"w"in c&&(c.w=0),null):(c.setScalar(0),c.addScaledVector(s,Qi.x),c.addScaledVector(o,Qi.y),c.addScaledVector(a,Qi.z),c)}static getInterpolatedAttribute(e,t,i,r,s,o){return Im.setScalar(0),Am.setScalar(0),Rm.setScalar(0),Im.fromBufferAttribute(e,t),Am.fromBufferAttribute(e,i),Rm.fromBufferAttribute(e,r),o.setScalar(0),o.addScaledVector(Im,s.x),o.addScaledVector(Am,s.y),o.addScaledVector(Rm,s.z),o}static isFrontFacing(e,t,i,r){return di.subVectors(i,t),Ki.subVectors(e,t),di.cross(Ki).dot(r)<0}set(e,t,i){return this.a.copy(e),this.b.copy(t),this.c.copy(i),this}setFromPointsAndIndices(e,t,i,r){return this.a.copy(e[t]),this.b.copy(e[i]),this.c.copy(e[r]),this}setFromAttributeAndIndices(e,t,i,r){return this.a.fromBufferAttribute(e,t),this.b.fromBufferAttribute(e,i),this.c.fromBufferAttribute(e,r),this}clone(){return new this.constructor().copy(this)}copy(e){return this.a.copy(e.a),this.b.copy(e.b),this.c.copy(e.c),this}getArea(){return di.subVectors(this.c,this.b),Ki.subVectors(this.a,this.b),di.cross(Ki).length()*.5}getMidpoint(e){return e.addVectors(this.a,this.b).add(this.c).multiplyScalar(1/3)}getNormal(e){return n.getNormal(this.a,this.b,this.c,e)}getPlane(e){return e.setFromCoplanarPoints(this.a,this.b,this.c)}getBarycoord(e,t){return n.getBarycoord(e,this.a,this.b,this.c,t)}getInterpolation(e,t,i,r,s){return n.getInterpolation(e,this.a,this.b,this.c,t,i,r,s)}containsPoint(e){return n.containsPoint(e,this.a,this.b,this.c)}isFrontFacing(e){return n.isFrontFacing(this.a,this.b,this.c,e)}intersectsBox(e){return e.intersectsTriangle(this)}closestPointToPoint(e,t){let i=this.a,r=this.b,s=this.c,o,a;vo.subVectors(r,i),yo.subVectors(s,i),Cm.subVectors(e,i);let c=vo.dot(Cm),l=yo.dot(Cm);if(c<=0&&l<=0)return t.copy(i);wm.subVectors(e,r);let u=vo.dot(wm),f=yo.dot(wm);if(u>=0&&f<=u)return t.copy(r);let d=c*f-u*l;if(d<=0&&c>=0&&u<=0)return o=c/(c-u),t.copy(i).addScaledVector(vo,o);Dm.subVectors(e,s);let h=vo.dot(Dm),g=yo.dot(Dm);if(g>=0&&h<=g)return t.copy(s);let x=h*l-c*g;if(x<=0&&l>=0&&g<=0)return a=l/(l-g),t.copy(i).addScaledVector(yo,a);let m=u*g-h*f;if(m<=0&&f-u>=0&&h-g>=0)return cx.subVectors(s,r),a=(f-u)/(f-u+(h-g)),t.copy(r).addScaledVector(cx,a);let p=1/(m+x+d);return o=x*p,a=d*p,t.copy(i).addScaledVector(vo,o).addScaledVector(yo,a)}equals(e){return e.a.equals(this.a)&&e.b.equals(this.b)&&e.c.equals(this.c)}},Lr=class{constructor(e=new k(1/0,1/0,1/0),t=new k(-1/0,-1/0,-1/0)){this.isBox3=!0,this.min=e,this.max=t}set(e,t){return this.min.copy(e),this.max.copy(t),this}setFromArray(e){this.makeEmpty();for(let t=0,i=e.length;t<i;t+=3)this.expandByPoint(fi.fromArray(e,t));return this}setFromBufferAttribute(e){this.makeEmpty();for(let t=0,i=e.count;t<i;t++)this.expandByPoint(fi.fromBufferAttribute(e,t));return this}setFromPoints(e){this.makeEmpty();for(let t=0,i=e.length;t<i;t++)this.expandByPoint(e[t]);return this}setFromCenterAndSize(e,t){let i=fi.copy(t).multiplyScalar(.5);return this.min.copy(e).sub(i),this.max.copy(e).add(i),this}setFromObject(e,t=!1){return this.makeEmpty(),this.expandByObject(e,t)}clone(){return new this.constructor().copy(this)}copy(e){return this.min.copy(e.min),this.max.copy(e.max),this}makeEmpty(){return this.min.x=this.min.y=this.min.z=1/0,this.max.x=this.max.y=this.max.z=-1/0,this}isEmpty(){return this.max.x<this.min.x||this.max.y<this.min.y||this.max.z<this.min.z}getCenter(e){return this.isEmpty()?e.set(0,0,0):e.addVectors(this.min,this.max).multiplyScalar(.5)}getSize(e){return this.isEmpty()?e.set(0,0,0):e.subVectors(this.max,this.min)}expandByPoint(e){return this.min.min(e),this.max.max(e),this}expandByVector(e){return this.min.sub(e),this.max.add(e),this}expandByScalar(e){return this.min.addScalar(-e),this.max.addScalar(e),this}expandByObject(e,t=!1){e.updateWorldMatrix(!1,!1);let i=e.geometry;if(i!==void 0){let s=i.getAttribute("position");if(t===!0&&s!==void 0&&e.isInstancedMesh!==!0)for(let o=0,a=s.count;o<a;o++)e.isMesh===!0?e.getVertexPosition(o,fi):fi.fromBufferAttribute(s,o),fi.applyMatrix4(e.matrixWorld),this.expandByPoint(fi);else e.boundingBox!==void 0?(e.boundingBox===null&&e.computeBoundingBox(),au.copy(e.boundingBox)):(i.boundingBox===null&&i.computeBoundingBox(),au.copy(i.boundingBox)),au.applyMatrix4(e.matrixWorld),this.union(au)}let r=e.children;for(let s=0,o=r.length;s<o;s++)this.expandByObject(r[s],t);return this}containsPoint(e){return e.x>=this.min.x&&e.x<=this.max.x&&e.y>=this.min.y&&e.y<=this.max.y&&e.z>=this.min.z&&e.z<=this.max.z}containsBox(e){return this.min.x<=e.min.x&&e.max.x<=this.max.x&&this.min.y<=e.min.y&&e.max.y<=this.max.y&&this.min.z<=e.min.z&&e.max.z<=this.max.z}getParameter(e,t){return t.set((e.x-this.min.x)/(this.max.x-this.min.x),(e.y-this.min.y)/(this.max.y-this.min.y),(e.z-this.min.z)/(this.max.z-this.min.z))}intersectsBox(e){return e.max.x>=this.min.x&&e.min.x<=this.max.x&&e.max.y>=this.min.y&&e.min.y<=this.max.y&&e.max.z>=this.min.z&&e.min.z<=this.max.z}intersectsSphere(e){return this.clampPoint(e.center,fi),fi.distanceToSquared(e.center)<=e.radius*e.radius}intersectsPlane(e){let t,i;return e.normal.x>0?(t=e.normal.x*this.min.x,i=e.normal.x*this.max.x):(t=e.normal.x*this.max.x,i=e.normal.x*this.min.x),e.normal.y>0?(t+=e.normal.y*this.min.y,i+=e.normal.y*this.max.y):(t+=e.normal.y*this.max.y,i+=e.normal.y*this.min.y),e.normal.z>0?(t+=e.normal.z*this.min.z,i+=e.normal.z*this.max.z):(t+=e.normal.z*this.max.z,i+=e.normal.z*this.min.z),t<=-e.constant&&i>=-e.constant}intersectsTriangle(e){if(this.isEmpty())return!1;this.getCenter(La),cu.subVectors(this.max,La),_o.subVectors(e.a,La),xo.subVectors(e.b,La),Mo.subVectors(e.c,La),Dr.subVectors(xo,_o),Ir.subVectors(Mo,xo),gs.subVectors(_o,Mo);let t=[0,-Dr.z,Dr.y,0,-Ir.z,Ir.y,0,-gs.z,gs.y,Dr.z,0,-Dr.x,Ir.z,0,-Ir.x,gs.z,0,-gs.x,-Dr.y,Dr.x,0,-Ir.y,Ir.x,0,-gs.y,gs.x,0];return!Nm(t,_o,xo,Mo,cu)||(t=[1,0,0,0,1,0,0,0,1],!Nm(t,_o,xo,Mo,cu))?!1:(lu.crossVectors(Dr,Ir),t=[lu.x,lu.y,lu.z],Nm(t,_o,xo,Mo,cu))}clampPoint(e,t){return t.copy(e).clamp(this.min,this.max)}distanceToPoint(e){return this.clampPoint(e,fi).distanceTo(e)}getBoundingSphere(e){return this.isEmpty()?e.makeEmpty():(this.getCenter(e.center),e.radius=this.getSize(fi).length()*.5),e}intersect(e){return this.min.max(e.min),this.max.min(e.max),this.isEmpty()&&this.makeEmpty(),this}union(e){return this.min.min(e.min),this.max.max(e.max),this}applyMatrix4(e){return this.isEmpty()?this:(er[0].set(this.min.x,this.min.y,this.min.z).applyMatrix4(e),er[1].set(this.min.x,this.min.y,this.max.z).applyMatrix4(e),er[2].set(this.min.x,this.max.y,this.min.z).applyMatrix4(e),er[3].set(this.min.x,this.max.y,this.max.z).applyMatrix4(e),er[4].set(this.max.x,this.min.y,this.min.z).applyMatrix4(e),er[5].set(this.max.x,this.min.y,this.max.z).applyMatrix4(e),er[6].set(this.max.x,this.max.y,this.min.z).applyMatrix4(e),er[7].set(this.max.x,this.max.y,this.max.z).applyMatrix4(e),this.setFromPoints(er),this)}translate(e){return this.min.add(e),this.max.add(e),this}equals(e){return e.min.equals(this.min)&&e.max.equals(this.max)}toJSON(){return{min:this.min.toArray(),max:this.max.toArray()}}fromJSON(e){return this.min.fromArray(e.min),this.max.fromArray(e.max),this}},er=[new k,new k,new k,new k,new k,new k,new k,new k],fi=new k,au=new Lr,_o=new k,xo=new k,Mo=new k,Dr=new k,Ir=new k,gs=new k,La=new k,cu=new k,lu=new k,vs=new k;function Nm(n,e,t,i,r){for(let s=0,o=n.length-3;s<=o;s+=3){vs.fromArray(n,s);let a=r.x*Math.abs(vs.x)+r.y*Math.abs(vs.y)+r.z*Math.abs(vs.z),c=e.dot(vs),l=t.dot(vs),u=i.dot(vs);if(Math.max(-Math.max(c,l,u),Math.min(c,l,u))>a)return!1}return!0}var Gt=new k,uu=new pt,Vw=0,pn=class{constructor(e,t,i=!1){if(Array.isArray(e))throw new TypeError("THREE.BufferAttribute: array should be a Typed Array.");this.isBufferAttribute=!0,Object.defineProperty(this,"id",{value:Vw++}),this.name="",this.array=e,this.itemSize=t,this.count=e!==void 0?e.length/t:0,this.normalized=i,this.usage=$m,this.updateRanges=[],this.gpuType=gi,this.version=0}onUploadCallback(){}set needsUpdate(e){e===!0&&this.version++}setUsage(e){return this.usage=e,this}addUpdateRange(e,t){this.updateRanges.push({start:e,count:t})}clearUpdateRanges(){this.updateRanges.length=0}copy(e){return this.name=e.name,this.array=new e.array.constructor(e.array),this.itemSize=e.itemSize,this.count=e.count,this.normalized=e.normalized,this.usage=e.usage,this.gpuType=e.gpuType,this}copyAt(e,t,i){e*=this.itemSize,i*=t.itemSize;for(let r=0,s=this.itemSize;r<s;r++)this.array[e+r]=t.array[i+r];return this}copyArray(e){return this.array.set(e),this}applyMatrix3(e){if(this.itemSize===2)for(let t=0,i=this.count;t<i;t++)uu.fromBufferAttribute(this,t),uu.applyMatrix3(e),this.setXY(t,uu.x,uu.y);else if(this.itemSize===3)for(let t=0,i=this.count;t<i;t++)Gt.fromBufferAttribute(this,t),Gt.applyMatrix3(e),this.setXYZ(t,Gt.x,Gt.y,Gt.z);return this}applyMatrix4(e){for(let t=0,i=this.count;t<i;t++)Gt.fromBufferAttribute(this,t),Gt.applyMatrix4(e),this.setXYZ(t,Gt.x,Gt.y,Gt.z);return this}applyNormalMatrix(e){for(let t=0,i=this.count;t<i;t++)Gt.fromBufferAttribute(this,t),Gt.applyNormalMatrix(e),this.setXYZ(t,Gt.x,Gt.y,Gt.z);return this}transformDirection(e){for(let t=0,i=this.count;t<i;t++)Gt.fromBufferAttribute(this,t),Gt.transformDirection(e),this.setXYZ(t,Gt.x,Gt.y,Gt.z);return this}set(e,t=0){return this.array.set(e,t),this}getComponent(e,t){let i=this.array[e*this.itemSize+t];return this.normalized&&(i=Na(i,this.array)),i}setComponent(e,t,i){return this.normalized&&(i=Sn(i,this.array)),this.array[e*this.itemSize+t]=i,this}getX(e){let t=this.array[e*this.itemSize];return this.normalized&&(t=Na(t,this.array)),t}setX(e,t){return this.normalized&&(t=Sn(t,this.array)),this.array[e*this.itemSize]=t,this}getY(e){let t=this.array[e*this.itemSize+1];return this.normalized&&(t=Na(t,this.array)),t}setY(e,t){return this.normalized&&(t=Sn(t,this.array)),this.array[e*this.itemSize+1]=t,this}getZ(e){let t=this.array[e*this.itemSize+2];return this.normalized&&(t=Na(t,this.array)),t}setZ(e,t){return this.normalized&&(t=Sn(t,this.array)),this.array[e*this.itemSize+2]=t,this}getW(e){let t=this.array[e*this.itemSize+3];return this.normalized&&(t=Na(t,this.array)),t}setW(e,t){return this.normalized&&(t=Sn(t,this.array)),this.array[e*this.itemSize+3]=t,this}setXY(e,t,i){return e*=this.itemSize,this.normalized&&(t=Sn(t,this.array),i=Sn(i,this.array)),this.array[e+0]=t,this.array[e+1]=i,this}setXYZ(e,t,i,r){return e*=this.itemSize,this.normalized&&(t=Sn(t,this.array),i=Sn(i,this.array),r=Sn(r,this.array)),this.array[e+0]=t,this.array[e+1]=i,this.array[e+2]=r,this}setXYZW(e,t,i,r,s){return e*=this.itemSize,this.normalized&&(t=Sn(t,this.array),i=Sn(i,this.array),r=Sn(r,this.array),s=Sn(s,this.array)),this.array[e+0]=t,this.array[e+1]=i,this.array[e+2]=r,this.array[e+3]=s,this}onUpload(e){return this.onUploadCallback=e,this}clone(){return new this.constructor(this.array,this.itemSize).copy(this)}toJSON(){let e={itemSize:this.itemSize,type:this.array.constructor.name,array:Array.from(this.array),normalized:this.normalized};return this.name!==""&&(e.name=this.name),this.usage!==$m&&(e.usage=this.usage),e}};var ja=class extends pn{constructor(e,t,i){super(new Uint16Array(e),t,i)}};var $a=class extends pn{constructor(e,t,i){super(new Uint32Array(e),t,i)}};var Yn=class extends pn{constructor(e,t,i){super(new Float32Array(e),t,i)}},Hw=new Lr,Fa=new k,Pm=new k,Ts=class{constructor(e=new k,t=-1){this.isSphere=!0,this.center=e,this.radius=t}set(e,t){return this.center.copy(e),this.radius=t,this}setFromPoints(e,t){let i=this.center;t!==void 0?i.copy(t):Hw.setFromPoints(e).getCenter(i);let r=0;for(let s=0,o=e.length;s<o;s++)r=Math.max(r,i.distanceToSquared(e[s]));return this.radius=Math.sqrt(r),this}copy(e){return this.center.copy(e.center),this.radius=e.radius,this}isEmpty(){return this.radius<0}makeEmpty(){return this.center.set(0,0,0),this.radius=-1,this}containsPoint(e){return e.distanceToSquared(this.center)<=this.radius*this.radius}distanceToPoint(e){return e.distanceTo(this.center)-this.radius}intersectsSphere(e){let t=this.radius+e.radius;return e.center.distanceToSquared(this.center)<=t*t}intersectsBox(e){return e.intersectsSphere(this)}intersectsPlane(e){return Math.abs(e.distanceToPoint(this.center))<=this.radius}clampPoint(e,t){let i=this.center.distanceToSquared(e);return t.copy(e),i>this.radius*this.radius&&(t.sub(this.center).normalize(),t.multiplyScalar(this.radius).add(this.center)),t}getBoundingBox(e){return this.isEmpty()?(e.makeEmpty(),e):(e.set(this.center,this.center),e.expandByScalar(this.radius),e)}applyMatrix4(e){return this.center.applyMatrix4(e),this.radius=this.radius*e.getMaxScaleOnAxis(),this}translate(e){return this.center.add(e),this}expandByPoint(e){if(this.isEmpty())return this.center.copy(e),this.radius=0,this;Fa.subVectors(e,this.center);let t=Fa.lengthSq();if(t>this.radius*this.radius){let i=Math.sqrt(t),r=(i-this.radius)*.5;this.center.addScaledVector(Fa,r/i),this.radius+=r}return this}union(e){return e.isEmpty()?this:this.isEmpty()?(this.copy(e),this):(this.center.equals(e.center)===!0?this.radius=Math.max(this.radius,e.radius):(Pm.subVectors(e.center,this.center).setLength(e.radius),this.expandByPoint(Fa.copy(e.center).add(Pm)),this.expandByPoint(Fa.copy(e.center).sub(Pm))),this)}equals(e){return e.center.equals(this.center)&&e.radius===this.radius}clone(){return new this.constructor().copy(this)}toJSON(){return{radius:this.radius,center:this.center.toArray()}}fromJSON(e){return this.radius=e.radius,this.center.fromArray(e.center),this}},zw=0,Xn=new Bt,Lm=new cr,bo=new k,Nn=new Lr,Oa=new Lr,Qt=new k,Fn=class n extends sr{constructor(){super(),this.isBufferGeometry=!0,Object.defineProperty(this,"id",{value:zw++}),this.uuid=dc(),this.name="",this.type="BufferGeometry",this.index=null,this.indirect=null,this.indirectOffset=0,this.attributes={},this.morphAttributes={},this.morphTargetsRelative=!1,this.groups=[],this.boundingBox=null,this.boundingSphere=null,this.drawRange={start:0,count:1/0},this.userData={}}getIndex(){return this.index}setIndex(e){return Array.isArray(e)?this.index=new(ww(e)?$a:ja)(e,1):this.index=e,this}setIndirect(e,t=0){return this.indirect=e,this.indirectOffset=t,this}getIndirect(){return this.indirect}getAttribute(e){return this.attributes[e]}setAttribute(e,t){return this.attributes[e]=t,this}deleteAttribute(e){return delete this.attributes[e],this}hasAttribute(e){return this.attributes[e]!==void 0}addGroup(e,t,i=0){this.groups.push({start:e,count:t,materialIndex:i})}clearGroups(){this.groups=[]}setDrawRange(e,t){this.drawRange.start=e,this.drawRange.count=t}applyMatrix4(e){let t=this.attributes.position;t!==void 0&&(t.applyMatrix4(e),t.needsUpdate=!0);let i=this.attributes.normal;if(i!==void 0){let s=new Be().getNormalMatrix(e);i.applyNormalMatrix(s),i.needsUpdate=!0}let r=this.attributes.tangent;return r!==void 0&&(r.transformDirection(e),r.needsUpdate=!0),this.boundingBox!==null&&this.computeBoundingBox(),this.boundingSphere!==null&&this.computeBoundingSphere(),this}applyQuaternion(e){return Xn.makeRotationFromQuaternion(e),this.applyMatrix4(Xn),this}rotateX(e){return Xn.makeRotationX(e),this.applyMatrix4(Xn),this}rotateY(e){return Xn.makeRotationY(e),this.applyMatrix4(Xn),this}rotateZ(e){return Xn.makeRotationZ(e),this.applyMatrix4(Xn),this}translate(e,t,i){return Xn.makeTranslation(e,t,i),this.applyMatrix4(Xn),this}scale(e,t,i){return Xn.makeScale(e,t,i),this.applyMatrix4(Xn),this}lookAt(e){return Lm.lookAt(e),Lm.updateMatrix(),this.applyMatrix4(Lm.matrix),this}center(){return this.computeBoundingBox(),this.boundingBox.getCenter(bo).negate(),this.translate(bo.x,bo.y,bo.z),this}setFromPoints(e){let t=this.getAttribute("position");if(t===void 0){let i=[];for(let r=0,s=e.length;r<s;r++){let o=e[r];i.push(o.x,o.y,o.z||0)}this.setAttribute("position",new Yn(i,3))}else{let i=Math.min(e.length,t.count);for(let r=0;r<i;r++){let s=e[r];t.setXYZ(r,s.x,s.y,s.z||0)}e.length>t.count&&Le("BufferGeometry: Buffer size too small for points data. Use .dispose() and create a new geometry."),t.needsUpdate=!0}return this}computeBoundingBox(){this.boundingBox===null&&(this.boundingBox=new Lr);let e=this.attributes.position,t=this.morphAttributes.position;if(e&&e.isGLBufferAttribute){Ne("BufferGeometry.computeBoundingBox(): GLBufferAttribute requires a manual bounding box.",this),this.boundingBox.set(new k(-1/0,-1/0,-1/0),new k(1/0,1/0,1/0));return}if(e!==void 0){if(this.boundingBox.setFromBufferAttribute(e),t)for(let i=0,r=t.length;i<r;i++){let s=t[i];Nn.setFromBufferAttribute(s),this.morphTargetsRelative?(Qt.addVectors(this.boundingBox.min,Nn.min),this.boundingBox.expandByPoint(Qt),Qt.addVectors(this.boundingBox.max,Nn.max),this.boundingBox.expandByPoint(Qt)):(this.boundingBox.expandByPoint(Nn.min),this.boundingBox.expandByPoint(Nn.max))}}else this.boundingBox.makeEmpty();(isNaN(this.boundingBox.min.x)||isNaN(this.boundingBox.min.y)||isNaN(this.boundingBox.min.z))&&Ne('BufferGeometry.computeBoundingBox(): Computed min/max have NaN values. The "position" attribute is likely to have NaN values.',this)}computeBoundingSphere(){this.boundingSphere===null&&(this.boundingSphere=new Ts);let e=this.attributes.position,t=this.morphAttributes.position;if(e&&e.isGLBufferAttribute){Ne("BufferGeometry.computeBoundingSphere(): GLBufferAttribute requires a manual bounding sphere.",this),this.boundingSphere.set(new k,1/0);return}if(e){let i=this.boundingSphere.center;if(Nn.setFromBufferAttribute(e),t)for(let s=0,o=t.length;s<o;s++){let a=t[s];Oa.setFromBufferAttribute(a),this.morphTargetsRelative?(Qt.addVectors(Nn.min,Oa.min),Nn.expandByPoint(Qt),Qt.addVectors(Nn.max,Oa.max),Nn.expandByPoint(Qt)):(Nn.expandByPoint(Oa.min),Nn.expandByPoint(Oa.max))}Nn.getCenter(i);let r=0;for(let s=0,o=e.count;s<o;s++)Qt.fromBufferAttribute(e,s),r=Math.max(r,i.distanceToSquared(Qt));if(t)for(let s=0,o=t.length;s<o;s++){let a=t[s],c=this.morphTargetsRelative;for(let l=0,u=a.count;l<u;l++)Qt.fromBufferAttribute(a,l),c&&(bo.fromBufferAttribute(e,l),Qt.add(bo)),r=Math.max(r,i.distanceToSquared(Qt))}this.boundingSphere.radius=Math.sqrt(r),isNaN(this.boundingSphere.radius)&&Ne('BufferGeometry.computeBoundingSphere(): Computed radius is NaN. The "position" attribute is likely to have NaN values.',this)}}computeTangents(){let e=this.index,t=this.attributes;if(e===null||t.position===void 0||t.normal===void 0||t.uv===void 0){Ne("BufferGeometry: .computeTangents() failed. Missing required attributes (index, position, normal or uv)");return}let i=t.position,r=t.normal,s=t.uv;this.hasAttribute("tangent")===!1&&this.setAttribute("tangent",new pn(new Float32Array(4*i.count),4));let o=this.getAttribute("tangent"),a=[],c=[];for(let y=0;y<i.count;y++)a[y]=new k,c[y]=new k;let l=new k,u=new k,f=new k,d=new pt,h=new pt,g=new pt,x=new k,m=new k;function p(y,E,q){l.fromBufferAttribute(i,y),u.fromBufferAttribute(i,E),f.fromBufferAttribute(i,q),d.fromBufferAttribute(s,y),h.fromBufferAttribute(s,E),g.fromBufferAttribute(s,q),u.sub(l),f.sub(l),h.sub(d),g.sub(d);let C=1/(h.x*g.y-g.x*h.y);isFinite(C)&&(x.copy(u).multiplyScalar(g.y).addScaledVector(f,-h.y).multiplyScalar(C),m.copy(f).multiplyScalar(h.x).addScaledVector(u,-g.x).multiplyScalar(C),a[y].add(x),a[E].add(x),a[q].add(x),c[y].add(m),c[E].add(m),c[q].add(m))}let M=this.groups;M.length===0&&(M=[{start:0,count:e.count}]);for(let y=0,E=M.length;y<E;++y){let q=M[y],C=q.start,U=q.count;for(let V=C,W=C+U;V<W;V+=3)p(e.getX(V+0),e.getX(V+1),e.getX(V+2))}let T=new k,S=new k,I=new k,D=new k;function A(y){I.fromBufferAttribute(r,y),D.copy(I);let E=a[y];T.copy(E),T.sub(I.multiplyScalar(I.dot(E))).normalize(),S.crossVectors(D,E);let C=S.dot(c[y])<0?-1:1;o.setXYZW(y,T.x,T.y,T.z,C)}for(let y=0,E=M.length;y<E;++y){let q=M[y],C=q.start,U=q.count;for(let V=C,W=C+U;V<W;V+=3)A(e.getX(V+0)),A(e.getX(V+1)),A(e.getX(V+2))}}computeVertexNormals(){let e=this.index,t=this.getAttribute("position");if(t!==void 0){let i=this.getAttribute("normal");if(i===void 0)i=new pn(new Float32Array(t.count*3),3),this.setAttribute("normal",i);else for(let d=0,h=i.count;d<h;d++)i.setXYZ(d,0,0,0);let r=new k,s=new k,o=new k,a=new k,c=new k,l=new k,u=new k,f=new k;if(e)for(let d=0,h=e.count;d<h;d+=3){let g=e.getX(d+0),x=e.getX(d+1),m=e.getX(d+2);r.fromBufferAttribute(t,g),s.fromBufferAttribute(t,x),o.fromBufferAttribute(t,m),u.subVectors(o,s),f.subVectors(r,s),u.cross(f),a.fromBufferAttribute(i,g),c.fromBufferAttribute(i,x),l.fromBufferAttribute(i,m),a.add(u),c.add(u),l.add(u),i.setXYZ(g,a.x,a.y,a.z),i.setXYZ(x,c.x,c.y,c.z),i.setXYZ(m,l.x,l.y,l.z)}else for(let d=0,h=t.count;d<h;d+=3)r.fromBufferAttribute(t,d+0),s.fromBufferAttribute(t,d+1),o.fromBufferAttribute(t,d+2),u.subVectors(o,s),f.subVectors(r,s),u.cross(f),i.setXYZ(d+0,u.x,u.y,u.z),i.setXYZ(d+1,u.x,u.y,u.z),i.setXYZ(d+2,u.x,u.y,u.z);this.normalizeNormals(),i.needsUpdate=!0}}normalizeNormals(){let e=this.attributes.normal;for(let t=0,i=e.count;t<i;t++)Qt.fromBufferAttribute(e,t),Qt.normalize(),e.setXYZ(t,Qt.x,Qt.y,Qt.z)}toNonIndexed(){function e(a,c){let l=a.array,u=a.itemSize,f=a.normalized,d=new l.constructor(c.length*u),h=0,g=0;for(let x=0,m=c.length;x<m;x++){a.isInterleavedBufferAttribute?h=c[x]*a.data.stride+a.offset:h=c[x]*u;for(let p=0;p<u;p++)d[g++]=l[h++]}return new pn(d,u,f)}if(this.index===null)return Le("BufferGeometry.toNonIndexed(): BufferGeometry is already non-indexed."),this;let t=new n,i=this.index.array,r=this.attributes;for(let a in r){let c=r[a],l=e(c,i);t.setAttribute(a,l)}let s=this.morphAttributes;for(let a in s){let c=[],l=s[a];for(let u=0,f=l.length;u<f;u++){let d=l[u],h=e(d,i);c.push(h)}t.morphAttributes[a]=c}t.morphTargetsRelative=this.morphTargetsRelative;let o=this.groups;for(let a=0,c=o.length;a<c;a++){let l=o[a];t.addGroup(l.start,l.count,l.materialIndex)}return t}toJSON(){let e={metadata:{version:4.7,type:"BufferGeometry",generator:"BufferGeometry.toJSON"}};if(e.uuid=this.uuid,e.type=this.type,this.name!==""&&(e.name=this.name),Object.keys(this.userData).length>0&&(e.userData=this.userData),this.parameters!==void 0){let c=this.parameters;for(let l in c)c[l]!==void 0&&(e[l]=c[l]);return e}e.data={attributes:{}};let t=this.index;t!==null&&(e.data.index={type:t.array.constructor.name,array:Array.prototype.slice.call(t.array)});let i=this.attributes;for(let c in i){let l=i[c];e.data.attributes[c]=l.toJSON(e.data)}let r={},s=!1;for(let c in this.morphAttributes){let l=this.morphAttributes[c],u=[];for(let f=0,d=l.length;f<d;f++){let h=l[f];u.push(h.toJSON(e.data))}u.length>0&&(r[c]=u,s=!0)}s&&(e.data.morphAttributes=r,e.data.morphTargetsRelative=this.morphTargetsRelative);let o=this.groups;o.length>0&&(e.data.groups=JSON.parse(JSON.stringify(o)));let a=this.boundingSphere;return a!==null&&(e.data.boundingSphere=a.toJSON()),e}clone(){return new this.constructor().copy(this)}copy(e){this.index=null,this.attributes={},this.morphAttributes={},this.groups=[],this.boundingBox=null,this.boundingSphere=null;let t={};this.name=e.name;let i=e.index;i!==null&&this.setIndex(i.clone());let r=e.attributes;for(let l in r){let u=r[l];this.setAttribute(l,u.clone(t))}let s=e.morphAttributes;for(let l in s){let u=[],f=s[l];for(let d=0,h=f.length;d<h;d++)u.push(f[d].clone(t));this.morphAttributes[l]=u}this.morphTargetsRelative=e.morphTargetsRelative;let o=e.groups;for(let l=0,u=o.length;l<u;l++){let f=o[l];this.addGroup(f.start,f.count,f.materialIndex)}let a=e.boundingBox;a!==null&&(this.boundingBox=a.clone());let c=e.boundingSphere;return c!==null&&(this.boundingSphere=c.clone()),this.drawRange.start=e.drawRange.start,this.drawRange.count=e.drawRange.count,this.userData=e.userData,this}dispose(){this.dispatchEvent({type:"dispose"})}};var Gw=0,Ii=class extends sr{constructor(){super(),this.isMaterial=!0,Object.defineProperty(this,"id",{value:Gw++}),this.uuid=dc(),this.name="",this.type="Material",this.blending=rr,this.side=ir,this.vertexColors=!1,this.opacity=1,this.transparent=!1,this.alphaHash=!1,this.blendSrc=wu,this.blendDst=Du,this.blendEquation=Pr,this.blendSrcAlpha=null,this.blendDstAlpha=null,this.blendEquationAlpha=null,this.blendColor=new Xe(0,0,0),this.blendAlpha=0,this.depthFunc=bs,this.depthTest=!0,this.depthWrite=!0,this.stencilWriteMask=255,this.stencilFunc=jm,this.stencilRef=0,this.stencilFuncMask=255,this.stencilFail=xs,this.stencilZFail=xs,this.stencilZPass=xs,this.stencilWrite=!1,this.clippingPlanes=null,this.clipIntersection=!1,this.clipShadows=!1,this.shadowSide=null,this.colorWrite=!0,this.precision=null,this.polygonOffset=!1,this.polygonOffsetFactor=0,this.polygonOffsetUnits=0,this.dithering=!1,this.alphaToCoverage=!1,this.premultipliedAlpha=!1,this.forceSinglePass=!1,this.allowOverride=!0,this.visible=!0,this.toneMapped=!0,this.userData={},this.version=0,this._alphaTest=0}get alphaTest(){return this._alphaTest}set alphaTest(e){this._alphaTest>0!=e>0&&this.version++,this._alphaTest=e}onBeforeRender(){}onBeforeCompile(){}customProgramCacheKey(){return this.onBeforeCompile.toString()}setValues(e){if(e!==void 0)for(let t in e){let i=e[t];if(i===void 0){Le(`Material: parameter '${t}' has value of undefined.`);continue}let r=this[t];if(r===void 0){Le(`Material: '${t}' is not a property of THREE.${this.type}.`);continue}r&&r.isColor?r.set(i):r&&r.isVector3&&i&&i.isVector3?r.copy(i):this[t]=i}}toJSON(e){let t=e===void 0||typeof e=="string";t&&(e={textures:{},images:{}});let i={metadata:{version:4.7,type:"Material",generator:"Material.toJSON"}};i.uuid=this.uuid,i.type=this.type,this.name!==""&&(i.name=this.name),this.color&&this.color.isColor&&(i.color=this.color.getHex()),this.roughness!==void 0&&(i.roughness=this.roughness),this.metalness!==void 0&&(i.metalness=this.metalness),this.sheen!==void 0&&(i.sheen=this.sheen),this.sheenColor&&this.sheenColor.isColor&&(i.sheenColor=this.sheenColor.getHex()),this.sheenRoughness!==void 0&&(i.sheenRoughness=this.sheenRoughness),this.emissive&&this.emissive.isColor&&(i.emissive=this.emissive.getHex()),this.emissiveIntensity!==void 0&&this.emissiveIntensity!==1&&(i.emissiveIntensity=this.emissiveIntensity),this.specular&&this.specular.isColor&&(i.specular=this.specular.getHex()),this.specularIntensity!==void 0&&(i.specularIntensity=this.specularIntensity),this.specularColor&&this.specularColor.isColor&&(i.specularColor=this.specularColor.getHex()),this.shininess!==void 0&&(i.shininess=this.shininess),this.clearcoat!==void 0&&(i.clearcoat=this.clearcoat),this.clearcoatRoughness!==void 0&&(i.clearcoatRoughness=this.clearcoatRoughness),this.clearcoatMap&&this.clearcoatMap.isTexture&&(i.clearcoatMap=this.clearcoatMap.toJSON(e).uuid),this.clearcoatRoughnessMap&&this.clearcoatRoughnessMap.isTexture&&(i.clearcoatRoughnessMap=this.clearcoatRoughnessMap.toJSON(e).uuid),this.clearcoatNormalMap&&this.clearcoatNormalMap.isTexture&&(i.clearcoatNormalMap=this.clearcoatNormalMap.toJSON(e).uuid,i.clearcoatNormalScale=this.clearcoatNormalScale.toArray()),this.sheenColorMap&&this.sheenColorMap.isTexture&&(i.sheenColorMap=this.sheenColorMap.toJSON(e).uuid),this.sheenRoughnessMap&&this.sheenRoughnessMap.isTexture&&(i.sheenRoughnessMap=this.sheenRoughnessMap.toJSON(e).uuid),this.dispersion!==void 0&&(i.dispersion=this.dispersion),this.iridescence!==void 0&&(i.iridescence=this.iridescence),this.iridescenceIOR!==void 0&&(i.iridescenceIOR=this.iridescenceIOR),this.iridescenceThicknessRange!==void 0&&(i.iridescenceThicknessRange=this.iridescenceThicknessRange),this.iridescenceMap&&this.iridescenceMap.isTexture&&(i.iridescenceMap=this.iridescenceMap.toJSON(e).uuid),this.iridescenceThicknessMap&&this.iridescenceThicknessMap.isTexture&&(i.iridescenceThicknessMap=this.iridescenceThicknessMap.toJSON(e).uuid),this.anisotropy!==void 0&&(i.anisotropy=this.anisotropy),this.anisotropyRotation!==void 0&&(i.anisotropyRotation=this.anisotropyRotation),this.anisotropyMap&&this.anisotropyMap.isTexture&&(i.anisotropyMap=this.anisotropyMap.toJSON(e).uuid),this.map&&this.map.isTexture&&(i.map=this.map.toJSON(e).uuid),this.matcap&&this.matcap.isTexture&&(i.matcap=this.matcap.toJSON(e).uuid),this.alphaMap&&this.alphaMap.isTexture&&(i.alphaMap=this.alphaMap.toJSON(e).uuid),this.lightMap&&this.lightMap.isTexture&&(i.lightMap=this.lightMap.toJSON(e).uuid,i.lightMapIntensity=this.lightMapIntensity),this.aoMap&&this.aoMap.isTexture&&(i.aoMap=this.aoMap.toJSON(e).uuid,i.aoMapIntensity=this.aoMapIntensity),this.bumpMap&&this.bumpMap.isTexture&&(i.bumpMap=this.bumpMap.toJSON(e).uuid,i.bumpScale=this.bumpScale),this.normalMap&&this.normalMap.isTexture&&(i.normalMap=this.normalMap.toJSON(e).uuid,i.normalMapType=this.normalMapType,i.normalScale=this.normalScale.toArray()),this.displacementMap&&this.displacementMap.isTexture&&(i.displacementMap=this.displacementMap.toJSON(e).uuid,i.displacementScale=this.displacementScale,i.displacementBias=this.displacementBias),this.roughnessMap&&this.roughnessMap.isTexture&&(i.roughnessMap=this.roughnessMap.toJSON(e).uuid),this.metalnessMap&&this.metalnessMap.isTexture&&(i.metalnessMap=this.metalnessMap.toJSON(e).uuid),this.emissiveMap&&this.emissiveMap.isTexture&&(i.emissiveMap=this.emissiveMap.toJSON(e).uuid),this.specularMap&&this.specularMap.isTexture&&(i.specularMap=this.specularMap.toJSON(e).uuid),this.specularIntensityMap&&this.specularIntensityMap.isTexture&&(i.specularIntensityMap=this.specularIntensityMap.toJSON(e).uuid),this.specularColorMap&&this.specularColorMap.isTexture&&(i.specularColorMap=this.specularColorMap.toJSON(e).uuid),this.envMap&&this.envMap.isTexture&&(i.envMap=this.envMap.toJSON(e).uuid,this.combine!==void 0&&(i.combine=this.combine)),this.envMapRotation!==void 0&&(i.envMapRotation=this.envMapRotation.toArray()),this.envMapIntensity!==void 0&&(i.envMapIntensity=this.envMapIntensity),this.reflectivity!==void 0&&(i.reflectivity=this.reflectivity),this.refractionRatio!==void 0&&(i.refractionRatio=this.refractionRatio),this.gradientMap&&this.gradientMap.isTexture&&(i.gradientMap=this.gradientMap.toJSON(e).uuid),this.transmission!==void 0&&(i.transmission=this.transmission),this.transmissionMap&&this.transmissionMap.isTexture&&(i.transmissionMap=this.transmissionMap.toJSON(e).uuid),this.thickness!==void 0&&(i.thickness=this.thickness),this.thicknessMap&&this.thicknessMap.isTexture&&(i.thicknessMap=this.thicknessMap.toJSON(e).uuid),this.attenuationDistance!==void 0&&this.attenuationDistance!==1/0&&(i.attenuationDistance=this.attenuationDistance),this.attenuationColor!==void 0&&(i.attenuationColor=this.attenuationColor.getHex()),this.size!==void 0&&(i.size=this.size),this.shadowSide!==null&&(i.shadowSide=this.shadowSide),this.sizeAttenuation!==void 0&&(i.sizeAttenuation=this.sizeAttenuation),this.blending!==rr&&(i.blending=this.blending),this.side!==ir&&(i.side=this.side),this.vertexColors===!0&&(i.vertexColors=!0),this.opacity<1&&(i.opacity=this.opacity),this.transparent===!0&&(i.transparent=!0),this.blendSrc!==wu&&(i.blendSrc=this.blendSrc),this.blendDst!==Du&&(i.blendDst=this.blendDst),this.blendEquation!==Pr&&(i.blendEquation=this.blendEquation),this.blendSrcAlpha!==null&&(i.blendSrcAlpha=this.blendSrcAlpha),this.blendDstAlpha!==null&&(i.blendDstAlpha=this.blendDstAlpha),this.blendEquationAlpha!==null&&(i.blendEquationAlpha=this.blendEquationAlpha),this.blendColor&&this.blendColor.isColor&&(i.blendColor=this.blendColor.getHex()),this.blendAlpha!==0&&(i.blendAlpha=this.blendAlpha),this.depthFunc!==bs&&(i.depthFunc=this.depthFunc),this.depthTest===!1&&(i.depthTest=this.depthTest),this.depthWrite===!1&&(i.depthWrite=this.depthWrite),this.colorWrite===!1&&(i.colorWrite=this.colorWrite),this.stencilWriteMask!==255&&(i.stencilWriteMask=this.stencilWriteMask),this.stencilFunc!==jm&&(i.stencilFunc=this.stencilFunc),this.stencilRef!==0&&(i.stencilRef=this.stencilRef),this.stencilFuncMask!==255&&(i.stencilFuncMask=this.stencilFuncMask),this.stencilFail!==xs&&(i.stencilFail=this.stencilFail),this.stencilZFail!==xs&&(i.stencilZFail=this.stencilZFail),this.stencilZPass!==xs&&(i.stencilZPass=this.stencilZPass),this.stencilWrite===!0&&(i.stencilWrite=this.stencilWrite),this.rotation!==void 0&&this.rotation!==0&&(i.rotation=this.rotation),this.polygonOffset===!0&&(i.polygonOffset=!0),this.polygonOffsetFactor!==0&&(i.polygonOffsetFactor=this.polygonOffsetFactor),this.polygonOffsetUnits!==0&&(i.polygonOffsetUnits=this.polygonOffsetUnits),this.linewidth!==void 0&&this.linewidth!==1&&(i.linewidth=this.linewidth),this.dashSize!==void 0&&(i.dashSize=this.dashSize),this.gapSize!==void 0&&(i.gapSize=this.gapSize),this.scale!==void 0&&(i.scale=this.scale),this.dithering===!0&&(i.dithering=!0),this.alphaTest>0&&(i.alphaTest=this.alphaTest),this.alphaHash===!0&&(i.alphaHash=!0),this.alphaToCoverage===!0&&(i.alphaToCoverage=!0),this.premultipliedAlpha===!0&&(i.premultipliedAlpha=!0),this.forceSinglePass===!0&&(i.forceSinglePass=!0),this.allowOverride===!1&&(i.allowOverride=!1),this.wireframe===!0&&(i.wireframe=!0),this.wireframeLinewidth>1&&(i.wireframeLinewidth=this.wireframeLinewidth),this.wireframeLinecap!=="round"&&(i.wireframeLinecap=this.wireframeLinecap),this.wireframeLinejoin!=="round"&&(i.wireframeLinejoin=this.wireframeLinejoin),this.flatShading===!0&&(i.flatShading=!0),this.visible===!1&&(i.visible=!1),this.toneMapped===!1&&(i.toneMapped=!1),this.fog===!1&&(i.fog=!1),Object.keys(this.userData).length>0&&(i.userData=this.userData);function r(s){let o=[];for(let a in s){let c=s[a];delete c.metadata,o.push(c)}return o}if(t){let s=r(e.textures),o=r(e.images);s.length>0&&(i.textures=s),o.length>0&&(i.images=o)}return i}clone(){return new this.constructor().copy(this)}copy(e){this.name=e.name,this.blending=e.blending,this.side=e.side,this.vertexColors=e.vertexColors,this.opacity=e.opacity,this.transparent=e.transparent,this.blendSrc=e.blendSrc,this.blendDst=e.blendDst,this.blendEquation=e.blendEquation,this.blendSrcAlpha=e.blendSrcAlpha,this.blendDstAlpha=e.blendDstAlpha,this.blendEquationAlpha=e.blendEquationAlpha,this.blendColor.copy(e.blendColor),this.blendAlpha=e.blendAlpha,this.depthFunc=e.depthFunc,this.depthTest=e.depthTest,this.depthWrite=e.depthWrite,this.stencilWriteMask=e.stencilWriteMask,this.stencilFunc=e.stencilFunc,this.stencilRef=e.stencilRef,this.stencilFuncMask=e.stencilFuncMask,this.stencilFail=e.stencilFail,this.stencilZFail=e.stencilZFail,this.stencilZPass=e.stencilZPass,this.stencilWrite=e.stencilWrite;let t=e.clippingPlanes,i=null;if(t!==null){let r=t.length;i=new Array(r);for(let s=0;s!==r;++s)i[s]=t[s].clone()}return this.clippingPlanes=i,this.clipIntersection=e.clipIntersection,this.clipShadows=e.clipShadows,this.shadowSide=e.shadowSide,this.colorWrite=e.colorWrite,this.precision=e.precision,this.polygonOffset=e.polygonOffset,this.polygonOffsetFactor=e.polygonOffsetFactor,this.polygonOffsetUnits=e.polygonOffsetUnits,this.dithering=e.dithering,this.alphaTest=e.alphaTest,this.alphaHash=e.alphaHash,this.alphaToCoverage=e.alphaToCoverage,this.premultipliedAlpha=e.premultipliedAlpha,this.forceSinglePass=e.forceSinglePass,this.allowOverride=e.allowOverride,this.visible=e.visible,this.toneMapped=e.toneMapped,this.userData=JSON.parse(JSON.stringify(e.userData)),this}dispose(){this.dispatchEvent({type:"dispose"})}set needsUpdate(e){e===!0&&this.version++}};var tr=new k,Fm=new k,du=new k,Ar=new k,Om=new k,fu=new k,km=new k,qa=class{constructor(e=new k,t=new k(0,0,-1)){this.origin=e,this.direction=t}set(e,t){return this.origin.copy(e),this.direction.copy(t),this}copy(e){return this.origin.copy(e.origin),this.direction.copy(e.direction),this}at(e,t){return t.copy(this.origin).addScaledVector(this.direction,e)}lookAt(e){return this.direction.copy(e).sub(this.origin).normalize(),this}recast(e){return this.origin.copy(this.at(e,tr)),this}closestPointToPoint(e,t){t.subVectors(e,this.origin);let i=t.dot(this.direction);return i<0?t.copy(this.origin):t.copy(this.origin).addScaledVector(this.direction,i)}distanceToPoint(e){return Math.sqrt(this.distanceSqToPoint(e))}distanceSqToPoint(e){let t=tr.subVectors(e,this.origin).dot(this.direction);return t<0?this.origin.distanceToSquared(e):(tr.copy(this.origin).addScaledVector(this.direction,t),tr.distanceToSquared(e))}distanceSqToSegment(e,t,i,r){Fm.copy(e).add(t).multiplyScalar(.5),du.copy(t).sub(e).normalize(),Ar.copy(this.origin).sub(Fm);let s=e.distanceTo(t)*.5,o=-this.direction.dot(du),a=Ar.dot(this.direction),c=-Ar.dot(du),l=Ar.lengthSq(),u=Math.abs(1-o*o),f,d,h,g;if(u>0)if(f=o*c-a,d=o*a-c,g=s*u,f>=0)if(d>=-g)if(d<=g){let x=1/u;f*=x,d*=x,h=f*(f+o*d+2*a)+d*(o*f+d+2*c)+l}else d=s,f=Math.max(0,-(o*d+a)),h=-f*f+d*(d+2*c)+l;else d=-s,f=Math.max(0,-(o*d+a)),h=-f*f+d*(d+2*c)+l;else d<=-g?(f=Math.max(0,-(-o*s+a)),d=f>0?-s:Math.min(Math.max(-s,-c),s),h=-f*f+d*(d+2*c)+l):d<=g?(f=0,d=Math.min(Math.max(-s,-c),s),h=d*(d+2*c)+l):(f=Math.max(0,-(o*s+a)),d=f>0?s:Math.min(Math.max(-s,-c),s),h=-f*f+d*(d+2*c)+l);else d=o>0?-s:s,f=Math.max(0,-(o*d+a)),h=-f*f+d*(d+2*c)+l;return i&&i.copy(this.origin).addScaledVector(this.direction,f),r&&r.copy(Fm).addScaledVector(du,d),h}intersectSphere(e,t){tr.subVectors(e.center,this.origin);let i=tr.dot(this.direction),r=tr.dot(tr)-i*i,s=e.radius*e.radius;if(r>s)return null;let o=Math.sqrt(s-r),a=i-o,c=i+o;return c<0?null:a<0?this.at(c,t):this.at(a,t)}intersectsSphere(e){return e.radius<0?!1:this.distanceSqToPoint(e.center)<=e.radius*e.radius}distanceToPlane(e){let t=e.normal.dot(this.direction);if(t===0)return e.distanceToPoint(this.origin)===0?0:null;let i=-(this.origin.dot(e.normal)+e.constant)/t;return i>=0?i:null}intersectPlane(e,t){let i=this.distanceToPlane(e);return i===null?null:this.at(i,t)}intersectsPlane(e){let t=e.distanceToPoint(this.origin);return t===0||e.normal.dot(this.direction)*t<0}intersectBox(e,t){let i,r,s,o,a,c,l=1/this.direction.x,u=1/this.direction.y,f=1/this.direction.z,d=this.origin;return l>=0?(i=(e.min.x-d.x)*l,r=(e.max.x-d.x)*l):(i=(e.max.x-d.x)*l,r=(e.min.x-d.x)*l),u>=0?(s=(e.min.y-d.y)*u,o=(e.max.y-d.y)*u):(s=(e.max.y-d.y)*u,o=(e.min.y-d.y)*u),i>o||s>r||((s>i||isNaN(i))&&(i=s),(o<r||isNaN(r))&&(r=o),f>=0?(a=(e.min.z-d.z)*f,c=(e.max.z-d.z)*f):(a=(e.max.z-d.z)*f,c=(e.min.z-d.z)*f),i>c||a>r)||((a>i||i!==i)&&(i=a),(c<r||r!==r)&&(r=c),r<0)?null:this.at(i>=0?i:r,t)}intersectsBox(e){return this.intersectBox(e,tr)!==null}intersectTriangle(e,t,i,r,s){Om.subVectors(t,e),fu.subVectors(i,e),km.crossVectors(Om,fu);let o=this.direction.dot(km),a;if(o>0){if(r)return null;a=1}else if(o<0)a=-1,o=-o;else return null;Ar.subVectors(this.origin,e);let c=a*this.direction.dot(fu.crossVectors(Ar,fu));if(c<0)return null;let l=a*this.direction.dot(Om.cross(Ar));if(l<0||c+l>o)return null;let u=-a*Ar.dot(km);return u<0?null:this.at(u/o,s)}applyMatrix4(e){return this.origin.applyMatrix4(e),this.direction.transformDirection(e),this}equals(e){return e.origin.equals(this.origin)&&e.direction.equals(this.direction)}clone(){return new this.constructor().copy(this)}},Xa=class extends Ii{constructor(e){super(),this.isMeshBasicMaterial=!0,this.type="MeshBasicMaterial",this.color=new Xe(16777215),this.map=null,this.lightMap=null,this.lightMapIntensity=1,this.aoMap=null,this.aoMapIntensity=1,this.specularMap=null,this.alphaMap=null,this.envMap=null,this.envMapRotation=new Ss,this.combine=Qm,this.reflectivity=1,this.refractionRatio=.98,this.wireframe=!1,this.wireframeLinewidth=1,this.wireframeLinecap="round",this.wireframeLinejoin="round",this.fog=!0,this.setValues(e)}copy(e){return super.copy(e),this.color.copy(e.color),this.map=e.map,this.lightMap=e.lightMap,this.lightMapIntensity=e.lightMapIntensity,this.aoMap=e.aoMap,this.aoMapIntensity=e.aoMapIntensity,this.specularMap=e.specularMap,this.alphaMap=e.alphaMap,this.envMap=e.envMap,this.envMapRotation.copy(e.envMapRotation),this.combine=e.combine,this.reflectivity=e.reflectivity,this.refractionRatio=e.refractionRatio,this.wireframe=e.wireframe,this.wireframeLinewidth=e.wireframeLinewidth,this.wireframeLinecap=e.wireframeLinecap,this.wireframeLinejoin=e.wireframeLinejoin,this.fog=e.fog,this}},lx=new Bt,ys=new qa,hu=new Ts,ux=new k,pu=new k,mu=new k,gu=new k,Um=new k,vu=new k,dx=new k,yu=new k,Zn=class extends cr{constructor(e=new Fn,t=new Xa){super(),this.isMesh=!0,this.type="Mesh",this.geometry=e,this.material=t,this.morphTargetDictionary=void 0,this.morphTargetInfluences=void 0,this.count=1,this.updateMorphTargets()}copy(e,t){return super.copy(e,t),e.morphTargetInfluences!==void 0&&(this.morphTargetInfluences=e.morphTargetInfluences.slice()),e.morphTargetDictionary!==void 0&&(this.morphTargetDictionary=Object.assign({},e.morphTargetDictionary)),this.material=Array.isArray(e.material)?e.material.slice():e.material,this.geometry=e.geometry,this}updateMorphTargets(){let t=this.geometry.morphAttributes,i=Object.keys(t);if(i.length>0){let r=t[i[0]];if(r!==void 0){this.morphTargetInfluences=[],this.morphTargetDictionary={};for(let s=0,o=r.length;s<o;s++){let a=r[s].name||String(s);this.morphTargetInfluences.push(0),this.morphTargetDictionary[a]=s}}}}getVertexPosition(e,t){let i=this.geometry,r=i.attributes.position,s=i.morphAttributes.position,o=i.morphTargetsRelative;t.fromBufferAttribute(r,e);let a=this.morphTargetInfluences;if(s&&a){vu.set(0,0,0);for(let c=0,l=s.length;c<l;c++){let u=a[c],f=s[c];u!==0&&(Um.fromBufferAttribute(f,e),o?vu.addScaledVector(Um,u):vu.addScaledVector(Um.sub(t),u))}t.add(vu)}return t}raycast(e,t){let i=this.geometry,r=this.material,s=this.matrixWorld;r!==void 0&&(i.boundingSphere===null&&i.computeBoundingSphere(),hu.copy(i.boundingSphere),hu.applyMatrix4(s),ys.copy(e.ray).recast(e.near),!(hu.containsPoint(ys.origin)===!1&&(ys.intersectSphere(hu,ux)===null||ys.origin.distanceToSquared(ux)>(e.far-e.near)**2))&&(lx.copy(s).invert(),ys.copy(e.ray).applyMatrix4(lx),!(i.boundingBox!==null&&ys.intersectsBox(i.boundingBox)===!1)&&this._computeIntersections(e,t,ys)))}_computeIntersections(e,t,i){let r,s=this.geometry,o=this.material,a=s.index,c=s.attributes.position,l=s.attributes.uv,u=s.attributes.uv1,f=s.attributes.normal,d=s.groups,h=s.drawRange;if(a!==null)if(Array.isArray(o))for(let g=0,x=d.length;g<x;g++){let m=d[g],p=o[m.materialIndex],M=Math.max(m.start,h.start),T=Math.min(a.count,Math.min(m.start+m.count,h.start+h.count));for(let S=M,I=T;S<I;S+=3){let D=a.getX(S),A=a.getX(S+1),y=a.getX(S+2);r=_u(this,p,e,i,l,u,f,D,A,y),r&&(r.faceIndex=Math.floor(S/3),r.face.materialIndex=m.materialIndex,t.push(r))}}else{let g=Math.max(0,h.start),x=Math.min(a.count,h.start+h.count);for(let m=g,p=x;m<p;m+=3){let M=a.getX(m),T=a.getX(m+1),S=a.getX(m+2);r=_u(this,o,e,i,l,u,f,M,T,S),r&&(r.faceIndex=Math.floor(m/3),t.push(r))}}else if(c!==void 0)if(Array.isArray(o))for(let g=0,x=d.length;g<x;g++){let m=d[g],p=o[m.materialIndex],M=Math.max(m.start,h.start),T=Math.min(c.count,Math.min(m.start+m.count,h.start+h.count));for(let S=M,I=T;S<I;S+=3){let D=S,A=S+1,y=S+2;r=_u(this,p,e,i,l,u,f,D,A,y),r&&(r.faceIndex=Math.floor(S/3),r.face.materialIndex=m.materialIndex,t.push(r))}}else{let g=Math.max(0,h.start),x=Math.min(c.count,h.start+h.count);for(let m=g,p=x;m<p;m+=3){let M=m,T=m+1,S=m+2;r=_u(this,o,e,i,l,u,f,M,T,S),r&&(r.faceIndex=Math.floor(m/3),t.push(r))}}}};function Ww(n,e,t,i,r,s,o,a){let c;if(e.side===xn?c=i.intersectTriangle(o,s,r,!0,a):c=i.intersectTriangle(r,s,o,e.side===ir,a),c===null)return null;yu.copy(a),yu.applyMatrix4(n.matrixWorld);let l=t.ray.origin.distanceTo(yu);return l<t.near||l>t.far?null:{distance:l,point:yu.clone(),object:n}}function _u(n,e,t,i,r,s,o,a,c,l){n.getVertexPosition(a,pu),n.getVertexPosition(c,mu),n.getVertexPosition(l,gu);let u=Ww(n,e,t,i,pu,mu,gu,dx);if(u){let f=new k;Nr.getBarycoord(dx,pu,mu,gu,f),r&&(u.uv=Nr.getInterpolatedAttribute(r,a,c,l,f,new pt)),s&&(u.uv1=Nr.getInterpolatedAttribute(s,a,c,l,f,new pt)),o&&(u.normal=Nr.getInterpolatedAttribute(o,a,c,l,f,new k),u.normal.dot(i.direction)>0&&u.normal.multiplyScalar(-1));let d={a,b:c,c:l,normal:new k,materialIndex:0};Nr.getNormal(pu,mu,gu,d.normal),u.face=d,u.barycoord=f}return u}var Gu=class extends ar{constructor(e=null,t=1,i=1,r,s,o,a,c,l=en,u=en,f,d){super(null,o,a,c,l,u,r,s,f,d),this.isDataTexture=!0,this.image={data:e,width:t,height:i},this.generateMipmaps=!1,this.flipY=!1,this.unpackAlignment=1}};var Bm=new k,jw=new k,$w=new Be,Ti=class{constructor(e=new k(1,0,0),t=0){this.isPlane=!0,this.normal=e,this.constant=t}set(e,t){return this.normal.copy(e),this.constant=t,this}setComponents(e,t,i,r){return this.normal.set(e,t,i),this.constant=r,this}setFromNormalAndCoplanarPoint(e,t){return this.normal.copy(e),this.constant=-t.dot(this.normal),this}setFromCoplanarPoints(e,t,i){let r=Bm.subVectors(i,t).cross(jw.subVectors(e,t)).normalize();return this.setFromNormalAndCoplanarPoint(r,e),this}copy(e){return this.normal.copy(e.normal),this.constant=e.constant,this}normalize(){let e=1/this.normal.length();return this.normal.multiplyScalar(e),this.constant*=e,this}negate(){return this.constant*=-1,this.normal.negate(),this}distanceToPoint(e){return this.normal.dot(e)+this.constant}distanceToSphere(e){return this.distanceToPoint(e.center)-e.radius}projectPoint(e,t){return t.copy(e).addScaledVector(this.normal,-this.distanceToPoint(e))}intersectLine(e,t){let i=e.delta(Bm),r=this.normal.dot(i);if(r===0)return this.distanceToPoint(e.start)===0?t.copy(e.start):null;let s=-(e.start.dot(this.normal)+this.constant)/r;return s<0||s>1?null:t.copy(e.start).addScaledVector(i,s)}intersectsLine(e){let t=this.distanceToPoint(e.start),i=this.distanceToPoint(e.end);return t<0&&i>0||i<0&&t>0}intersectsBox(e){return e.intersectsPlane(this)}intersectsSphere(e){return e.intersectsPlane(this)}coplanarPoint(e){return e.copy(this.normal).multiplyScalar(-this.constant)}applyMatrix4(e,t){let i=t||$w.getNormalMatrix(e),r=this.coplanarPoint(Bm).applyMatrix4(e),s=this.normal.applyMatrix3(i).normalize();return this.constant=-r.dot(s),this}translate(e){return this.constant-=e.dot(this.normal),this}equals(e){return e.normal.equals(this.normal)&&e.constant===this.constant}clone(){return new this.constructor().copy(this)}},_s=new Ts,qw=new pt(.5,.5),xu=new k,Ya=class{constructor(e=new Ti,t=new Ti,i=new Ti,r=new Ti,s=new Ti,o=new Ti){this.planes=[e,t,i,r,s,o]}set(e,t,i,r,s,o){let a=this.planes;return a[0].copy(e),a[1].copy(t),a[2].copy(i),a[3].copy(r),a[4].copy(s),a[5].copy(o),this}copy(e){let t=this.planes;for(let i=0;i<6;i++)t[i].copy(e.planes[i]);return this}setFromProjectionMatrix(e,t=hi,i=!1){let r=this.planes,s=e.elements,o=s[0],a=s[1],c=s[2],l=s[3],u=s[4],f=s[5],d=s[6],h=s[7],g=s[8],x=s[9],m=s[10],p=s[11],M=s[12],T=s[13],S=s[14],I=s[15];if(r[0].setComponents(l-o,h-u,p-g,I-M).normalize(),r[1].setComponents(l+o,h+u,p+g,I+M).normalize(),r[2].setComponents(l+a,h+f,p+x,I+T).normalize(),r[3].setComponents(l-a,h-f,p-x,I-T).normalize(),i)r[4].setComponents(c,d,m,S).normalize(),r[5].setComponents(l-c,h-d,p-m,I-S).normalize();else if(r[4].setComponents(l-c,h-d,p-m,I-S).normalize(),t===hi)r[5].setComponents(l+c,h+d,p+m,I+S).normalize();else if(t===Ba)r[5].setComponents(c,d,m,S).normalize();else throw new Error("THREE.Frustum.setFromProjectionMatrix(): Invalid coordinate system: "+t);return this}intersectsObject(e){if(e.boundingSphere!==void 0)e.boundingSphere===null&&e.computeBoundingSphere(),_s.copy(e.boundingSphere).applyMatrix4(e.matrixWorld);else{let t=e.geometry;t.boundingSphere===null&&t.computeBoundingSphere(),_s.copy(t.boundingSphere).applyMatrix4(e.matrixWorld)}return this.intersectsSphere(_s)}intersectsSprite(e){_s.center.set(0,0,0);let t=qw.distanceTo(e.center);return _s.radius=.7071067811865476+t,_s.applyMatrix4(e.matrixWorld),this.intersectsSphere(_s)}intersectsSphere(e){let t=this.planes,i=e.center,r=-e.radius;for(let s=0;s<6;s++)if(t[s].distanceToPoint(i)<r)return!1;return!0}intersectsBox(e){let t=this.planes;for(let i=0;i<6;i++){let r=t[i];if(xu.x=r.normal.x>0?e.max.x:e.min.x,xu.y=r.normal.y>0?e.max.y:e.min.y,xu.z=r.normal.z>0?e.max.z:e.min.z,r.distanceToPoint(xu)<0)return!1}return!0}containsPoint(e){let t=this.planes;for(let i=0;i<6;i++)if(t[i].distanceToPoint(e)<0)return!1;return!0}clone(){return new this.constructor().copy(this)}};var Io=class extends Ii{constructor(e){super(),this.isPointsMaterial=!0,this.type="PointsMaterial",this.color=new Xe(16777215),this.map=null,this.alphaMap=null,this.size=1,this.sizeAttenuation=!0,this.fog=!0,this.setValues(e)}copy(e){return super.copy(e),this.color.copy(e.color),this.map=e.map,this.alphaMap=e.alphaMap,this.size=e.size,this.sizeAttenuation=e.sizeAttenuation,this.fog=e.fog,this}},fx=new Bt,qm=new qa,Mu=new Ts,bu=new k,Za=class extends cr{constructor(e=new Fn,t=new Io){super(),this.isPoints=!0,this.type="Points",this.geometry=e,this.material=t,this.morphTargetDictionary=void 0,this.morphTargetInfluences=void 0,this.updateMorphTargets()}copy(e,t){return super.copy(e,t),this.material=Array.isArray(e.material)?e.material.slice():e.material,this.geometry=e.geometry,this}raycast(e,t){let i=this.geometry,r=this.matrixWorld,s=e.params.Points.threshold,o=i.drawRange;if(i.boundingSphere===null&&i.computeBoundingSphere(),Mu.copy(i.boundingSphere),Mu.applyMatrix4(r),Mu.radius+=s,e.ray.intersectsSphere(Mu)===!1)return;fx.copy(r).invert(),qm.copy(e.ray).applyMatrix4(fx);let a=s/((this.scale.x+this.scale.y+this.scale.z)/3),c=a*a,l=i.index,f=i.attributes.position;if(l!==null){let d=Math.max(0,o.start),h=Math.min(l.count,o.start+o.count);for(let g=d,x=h;g<x;g++){let m=l.getX(g);bu.fromBufferAttribute(f,m),hx(bu,m,c,r,e,t,this)}}else{let d=Math.max(0,o.start),h=Math.min(f.count,o.start+o.count);for(let g=d,x=h;g<x;g++)bu.fromBufferAttribute(f,g),hx(bu,g,c,r,e,t,this)}}updateMorphTargets(){let t=this.geometry.morphAttributes,i=Object.keys(t);if(i.length>0){let r=t[i[0]];if(r!==void 0){this.morphTargetInfluences=[],this.morphTargetDictionary={};for(let s=0,o=r.length;s<o;s++){let a=r[s].name||String(s);this.morphTargetInfluences.push(0),this.morphTargetDictionary[a]=s}}}}};function hx(n,e,t,i,r,s,o){let a=qm.distanceSqToPoint(n);if(a<t){let c=new k;qm.closestPointToPoint(n,c),c.applyMatrix4(i);let l=r.ray.origin.distanceTo(c);if(l<r.near||l>r.far)return;s.push({distance:l,distanceToRay:Math.sqrt(a),point:c,index:e,face:null,faceIndex:null,barycoord:null,object:o})}}var Ja=class extends ar{constructor(e=[],t=Br,i,r,s,o,a,c,l,u){super(e,t,i,r,s,o,a,c,l,u),this.isCubeTexture=!0,this.flipY=!1}get images(){return this.image}set images(e){this.image=e}};var Fr=class extends ar{constructor(e,t,i=mi,r,s,o,a=en,c=en,l,u=wi,f=1){if(u!==wi&&u!==Hr)throw new Error("DepthTexture format must be either THREE.DepthFormat or THREE.DepthStencilFormat");let d={width:e,height:t,depth:f};super(d,r,s,o,a,c,u,i,l),this.isDepthTexture=!0,this.flipY=!1,this.generateMipmaps=!1,this.compareFunction=null}copy(e){return super.copy(e),this.source=new wo(Object.assign({},e.image)),this.compareFunction=e.compareFunction,this}toJSON(e){let t=super.toJSON(e);return this.compareFunction!==null&&(t.compareFunction=this.compareFunction),t}},Wu=class extends Fr{constructor(e,t=mi,i=Br,r,s,o=en,a=en,c,l=wi){let u={width:e,height:e,depth:1},f=[u,u,u,u,u,u];super(e,e,t,i,r,s,o,a,c,l),this.image=f,this.isCubeDepthTexture=!0,this.isCubeTexture=!0}get images(){return this.image}set images(e){this.image=e}},Ka=class extends ar{constructor(e=null){super(),this.sourceTexture=e,this.isExternalTexture=!0}copy(e){return super.copy(e),this.sourceTexture=e.sourceTexture,this}},Ao=class n extends Fn{constructor(e=1,t=1,i=1,r=1,s=1,o=1){super(),this.type="BoxGeometry",this.parameters={width:e,height:t,depth:i,widthSegments:r,heightSegments:s,depthSegments:o};let a=this;r=Math.floor(r),s=Math.floor(s),o=Math.floor(o);let c=[],l=[],u=[],f=[],d=0,h=0;g("z","y","x",-1,-1,i,t,e,o,s,0),g("z","y","x",1,-1,i,t,-e,o,s,1),g("x","z","y",1,1,e,i,t,r,o,2),g("x","z","y",1,-1,e,i,-t,r,o,3),g("x","y","z",1,-1,e,t,i,r,s,4),g("x","y","z",-1,-1,e,t,-i,r,s,5),this.setIndex(c),this.setAttribute("position",new Yn(l,3)),this.setAttribute("normal",new Yn(u,3)),this.setAttribute("uv",new Yn(f,2));function g(x,m,p,M,T,S,I,D,A,y,E){let q=S/A,C=I/y,U=S/2,V=I/2,W=D/2,B=A+1,H=y+1,L=0,Q=0,Z=new k;for(let ue=0;ue<H;ue++){let ge=ue*C-V;for(let fe=0;fe<B;fe++){let ze=fe*q-U;Z[x]=ze*M,Z[m]=ge*T,Z[p]=W,l.push(Z.x,Z.y,Z.z),Z[x]=0,Z[m]=0,Z[p]=D>0?1:-1,u.push(Z.x,Z.y,Z.z),f.push(fe/A),f.push(1-ue/y),L+=1}}for(let ue=0;ue<y;ue++)for(let ge=0;ge<A;ge++){let fe=d+ge+B*ue,ze=d+ge+B*(ue+1),Tt=d+(ge+1)+B*(ue+1),St=d+(ge+1)+B*ue;c.push(fe,ze,St),c.push(ze,Tt,St),Q+=6}a.addGroup(h,Q,E),h+=Q,d+=L}}copy(e){return super.copy(e),this.parameters=Object.assign({},e.parameters),this}static fromJSON(e){return new n(e.width,e.height,e.depth,e.widthSegments,e.heightSegments,e.depthSegments)}};var Qa=class n extends Fn{constructor(e=1,t=1,i=1,r=1){super(),this.type="PlaneGeometry",this.parameters={width:e,height:t,widthSegments:i,heightSegments:r};let s=e/2,o=t/2,a=Math.floor(i),c=Math.floor(r),l=a+1,u=c+1,f=e/a,d=t/c,h=[],g=[],x=[],m=[];for(let p=0;p<u;p++){let M=p*d-o;for(let T=0;T<l;T++){let S=T*f-s;g.push(S,-M,0),x.push(0,0,1),m.push(T/a),m.push(1-p/c)}}for(let p=0;p<c;p++)for(let M=0;M<a;M++){let T=M+l*p,S=M+l*(p+1),I=M+1+l*(p+1),D=M+1+l*p;h.push(T,S,D),h.push(S,I,D)}this.setIndex(h),this.setAttribute("position",new Yn(g,3)),this.setAttribute("normal",new Yn(x,3)),this.setAttribute("uv",new Yn(m,2))}copy(e){return super.copy(e),this.parameters=Object.assign({},e.parameters),this}static fromJSON(e){return new n(e.width,e.height,e.widthSegments,e.heightSegments)}};function Ds(n){let e={};for(let t in n){e[t]={};for(let i in n[t]){let r=n[t][i];r&&(r.isColor||r.isMatrix3||r.isMatrix4||r.isVector2||r.isVector3||r.isVector4||r.isTexture||r.isQuaternion)?r.isRenderTargetTexture?(Le("UniformsUtils: Textures of render targets cannot be cloned via cloneUniforms() or mergeUniforms()."),e[t][i]=null):e[t][i]=r.clone():Array.isArray(r)?e[t][i]=r.slice():e[t][i]=r}}return e}function mn(n){let e={};for(let t=0;t<n.length;t++){let i=Ds(n[t]);for(let r in i)e[r]=i[r]}return e}function Xw(n){let e=[];for(let t=0;t<n.length;t++)e.push(n[t].clone());return e}function gg(n){let e=n.getRenderTarget();return e===null?n.outputColorSpace:e.isXRRenderTarget===!0?e.texture.colorSpace:rt.workingColorSpace}var eM={clone:Ds,merge:mn},Yw=`void main() {
	gl_Position = projectionMatrix * modelViewMatrix * vec4( position, 1.0 );
}`,Zw=`void main() {
	gl_FragColor = vec4( 1.0, 0.0, 0.0, 1.0 );
}`,On=class extends Ii{constructor(e){super(),this.isShaderMaterial=!0,this.type="ShaderMaterial",this.defines={},this.uniforms={},this.uniformsGroups=[],this.vertexShader=Yw,this.fragmentShader=Zw,this.linewidth=1,this.wireframe=!1,this.wireframeLinewidth=1,this.fog=!1,this.lights=!1,this.clipping=!1,this.forceSinglePass=!0,this.extensions={clipCullDistance:!1,multiDraw:!1},this.defaultAttributeValues={color:[1,1,1],uv:[0,0],uv1:[0,0]},this.index0AttributeName=void 0,this.uniformsNeedUpdate=!1,this.glslVersion=null,e!==void 0&&this.setValues(e)}copy(e){return super.copy(e),this.fragmentShader=e.fragmentShader,this.vertexShader=e.vertexShader,this.uniforms=Ds(e.uniforms),this.uniformsGroups=Xw(e.uniformsGroups),this.defines=Object.assign({},e.defines),this.wireframe=e.wireframe,this.wireframeLinewidth=e.wireframeLinewidth,this.fog=e.fog,this.lights=e.lights,this.clipping=e.clipping,this.extensions=Object.assign({},e.extensions),this.glslVersion=e.glslVersion,this.defaultAttributeValues=Object.assign({},e.defaultAttributeValues),this.index0AttributeName=e.index0AttributeName,this.uniformsNeedUpdate=e.uniformsNeedUpdate,this}toJSON(e){let t=super.toJSON(e);t.glslVersion=this.glslVersion,t.uniforms={};for(let r in this.uniforms){let o=this.uniforms[r].value;o&&o.isTexture?t.uniforms[r]={type:"t",value:o.toJSON(e).uuid}:o&&o.isColor?t.uniforms[r]={type:"c",value:o.getHex()}:o&&o.isVector2?t.uniforms[r]={type:"v2",value:o.toArray()}:o&&o.isVector3?t.uniforms[r]={type:"v3",value:o.toArray()}:o&&o.isVector4?t.uniforms[r]={type:"v4",value:o.toArray()}:o&&o.isMatrix3?t.uniforms[r]={type:"m3",value:o.toArray()}:o&&o.isMatrix4?t.uniforms[r]={type:"m4",value:o.toArray()}:t.uniforms[r]={value:o}}Object.keys(this.defines).length>0&&(t.defines=this.defines),t.vertexShader=this.vertexShader,t.fragmentShader=this.fragmentShader,t.lights=this.lights,t.clipping=this.clipping;let i={};for(let r in this.extensions)this.extensions[r]===!0&&(i[r]=!0);return Object.keys(i).length>0&&(t.extensions=i),t}},ju=class extends On{constructor(e){super(e),this.isRawShaderMaterial=!0,this.type="RawShaderMaterial"}};var $u=class extends Ii{constructor(e){super(),this.isMeshDepthMaterial=!0,this.type="MeshDepthMaterial",this.depthPacking=Vx,this.map=null,this.alphaMap=null,this.displacementMap=null,this.displacementScale=1,this.displacementBias=0,this.wireframe=!1,this.wireframeLinewidth=1,this.setValues(e)}copy(e){return super.copy(e),this.depthPacking=e.depthPacking,this.map=e.map,this.alphaMap=e.alphaMap,this.displacementMap=e.displacementMap,this.displacementScale=e.displacementScale,this.displacementBias=e.displacementBias,this.wireframe=e.wireframe,this.wireframeLinewidth=e.wireframeLinewidth,this}},qu=class extends Ii{constructor(e){super(),this.isMeshDistanceMaterial=!0,this.type="MeshDistanceMaterial",this.map=null,this.alphaMap=null,this.displacementMap=null,this.displacementScale=1,this.displacementBias=0,this.setValues(e)}copy(e){return super.copy(e),this.map=e.map,this.alphaMap=e.alphaMap,this.displacementMap=e.displacementMap,this.displacementScale=e.displacementScale,this.displacementBias=e.displacementBias,this}};function Eu(n,e){return!n||n.constructor===e?n:typeof e.BYTES_PER_ELEMENT=="number"?new e(n):Array.prototype.slice.call(n)}var Or=class{constructor(e,t,i,r){this.parameterPositions=e,this._cachedIndex=0,this.resultBuffer=r!==void 0?r:new t.constructor(i),this.sampleValues=t,this.valueSize=i,this.settings=null,this.DefaultSettings_={}}evaluate(e){let t=this.parameterPositions,i=this._cachedIndex,r=t[i],s=t[i-1];n:{e:{let o;t:{i:if(!(e<r)){for(let a=i+2;;){if(r===void 0){if(e<s)break i;return i=t.length,this._cachedIndex=i,this.copySampleValue_(i-1)}if(i===a)break;if(s=r,r=t[++i],e<r)break e}o=t.length;break t}if(!(e>=s)){let a=t[1];e<a&&(i=2,s=a);for(let c=i-2;;){if(s===void 0)return this._cachedIndex=0,this.copySampleValue_(0);if(i===c)break;if(r=s,s=t[--i-1],e>=s)break e}o=i,i=0;break t}break n}for(;i<o;){let a=i+o>>>1;e<t[a]?o=a:i=a+1}if(r=t[i],s=t[i-1],s===void 0)return this._cachedIndex=0,this.copySampleValue_(0);if(r===void 0)return i=t.length,this._cachedIndex=i,this.copySampleValue_(i-1)}this._cachedIndex=i,this.intervalChanged_(i,s,r)}return this.interpolate_(i,s,e,r)}getSettings_(){return this.settings||this.DefaultSettings_}copySampleValue_(e){let t=this.resultBuffer,i=this.sampleValues,r=this.valueSize,s=e*r;for(let o=0;o!==r;++o)t[o]=i[s+o];return t}interpolate_(){throw new Error("call to abstract method")}intervalChanged_(){}},Xu=class extends Or{constructor(e,t,i,r){super(e,t,i,r),this._weightPrev=-0,this._offsetPrev=-0,this._weightNext=-0,this._offsetNext=-0,this.DefaultSettings_={endingStart:zm,endingEnd:zm}}intervalChanged_(e,t,i){let r=this.parameterPositions,s=e-2,o=e+1,a=r[s],c=r[o];if(a===void 0)switch(this.getSettings_().endingStart){case Gm:s=e,a=2*t-i;break;case Wm:s=r.length-2,a=t+r[s]-r[s+1];break;default:s=e,a=i}if(c===void 0)switch(this.getSettings_().endingEnd){case Gm:o=e,c=2*i-t;break;case Wm:o=1,c=i+r[1]-r[0];break;default:o=e-1,c=t}let l=(i-t)*.5,u=this.valueSize;this._weightPrev=l/(t-a),this._weightNext=l/(c-i),this._offsetPrev=s*u,this._offsetNext=o*u}interpolate_(e,t,i,r){let s=this.resultBuffer,o=this.sampleValues,a=this.valueSize,c=e*a,l=c-a,u=this._offsetPrev,f=this._offsetNext,d=this._weightPrev,h=this._weightNext,g=(i-t)/(r-t),x=g*g,m=x*g,p=-d*m+2*d*x-d*g,M=(1+d)*m+(-1.5-2*d)*x+(-.5+d)*g+1,T=(-1-h)*m+(1.5+h)*x+.5*g,S=h*m-h*x;for(let I=0;I!==a;++I)s[I]=p*o[u+I]+M*o[l+I]+T*o[c+I]+S*o[f+I];return s}},Yu=class extends Or{constructor(e,t,i,r){super(e,t,i,r)}interpolate_(e,t,i,r){let s=this.resultBuffer,o=this.sampleValues,a=this.valueSize,c=e*a,l=c-a,u=(i-t)/(r-t),f=1-u;for(let d=0;d!==a;++d)s[d]=o[l+d]*f+o[c+d]*u;return s}},Zu=class extends Or{constructor(e,t,i,r){super(e,t,i,r)}interpolate_(e){return this.copySampleValue_(e-1)}},Ju=class extends Or{interpolate_(e,t,i,r){let s=this.resultBuffer,o=this.sampleValues,a=this.valueSize,c=e*a,l=c-a,u=this.settings||this.DefaultSettings_,f=u.inTangents,d=u.outTangents;if(!f||!d){let x=(i-t)/(r-t),m=1-x;for(let p=0;p!==a;++p)s[p]=o[l+p]*m+o[c+p]*x;return s}let h=a*2,g=e-1;for(let x=0;x!==a;++x){let m=o[l+x],p=o[c+x],M=g*h+x*2,T=d[M],S=d[M+1],I=e*h+x*2,D=f[I],A=f[I+1],y=(i-t)/(r-t),E,q,C,U,V;for(let W=0;W<8;W++){E=y*y,q=E*y,C=1-y,U=C*C,V=U*C;let H=V*t+3*U*y*T+3*C*E*D+q*r-i;if(Math.abs(H)<1e-10)break;let L=3*U*(T-t)+6*C*y*(D-T)+3*E*(r-D);if(Math.abs(L)<1e-10)break;y=y-H/L,y=Math.max(0,Math.min(1,y))}s[x]=V*m+3*U*y*S+3*C*E*A+q*p}return s}},kn=class{constructor(e,t,i,r){if(e===void 0)throw new Error("THREE.KeyframeTrack: track name is undefined");if(t===void 0||t.length===0)throw new Error("THREE.KeyframeTrack: no keyframes in track named "+e);this.name=e,this.times=Eu(t,this.TimeBufferType),this.values=Eu(i,this.ValueBufferType),this.setInterpolation(r||this.DefaultInterpolation)}static toJSON(e){let t=e.constructor,i;if(t.toJSON!==this.toJSON)i=t.toJSON(e);else{i={name:e.name,times:Eu(e.times,Array),values:Eu(e.values,Array)};let r=e.getInterpolation();r!==e.DefaultInterpolation&&(i.interpolation=r)}return i.type=e.ValueTypeName,i}InterpolantFactoryMethodDiscrete(e){return new Zu(this.times,this.values,this.getValueSize(),e)}InterpolantFactoryMethodLinear(e){return new Yu(this.times,this.values,this.getValueSize(),e)}InterpolantFactoryMethodSmooth(e){return new Xu(this.times,this.values,this.getValueSize(),e)}InterpolantFactoryMethodBezier(e){let t=new Ju(this.times,this.values,this.getValueSize(),e);return this.settings&&(t.settings=this.settings),t}setInterpolation(e){let t;switch(e){case ka:t=this.InterpolantFactoryMethodDiscrete;break;case Uu:t=this.InterpolantFactoryMethodLinear;break;case Cu:t=this.InterpolantFactoryMethodSmooth;break;case Hm:t=this.InterpolantFactoryMethodBezier;break}if(t===void 0){let i="unsupported interpolation for "+this.ValueTypeName+" keyframe track named "+this.name;if(this.createInterpolant===void 0)if(e!==this.DefaultInterpolation)this.setInterpolation(this.DefaultInterpolation);else throw new Error(i);return Le("KeyframeTrack:",i),this}return this.createInterpolant=t,this}getInterpolation(){switch(this.createInterpolant){case this.InterpolantFactoryMethodDiscrete:return ka;case this.InterpolantFactoryMethodLinear:return Uu;case this.InterpolantFactoryMethodSmooth:return Cu;case this.InterpolantFactoryMethodBezier:return Hm}}getValueSize(){return this.values.length/this.times.length}shift(e){if(e!==0){let t=this.times;for(let i=0,r=t.length;i!==r;++i)t[i]+=e}return this}scale(e){if(e!==1){let t=this.times;for(let i=0,r=t.length;i!==r;++i)t[i]*=e}return this}trim(e,t){let i=this.times,r=i.length,s=0,o=r-1;for(;s!==r&&i[s]<e;)++s;for(;o!==-1&&i[o]>t;)--o;if(++o,s!==0||o!==r){s>=o&&(o=Math.max(o,1),s=o-1);let a=this.getValueSize();this.times=i.slice(s,o),this.values=this.values.slice(s*a,o*a)}return this}validate(){let e=!0,t=this.getValueSize();t-Math.floor(t)!==0&&(Ne("KeyframeTrack: Invalid value size in track.",this),e=!1);let i=this.times,r=this.values,s=i.length;s===0&&(Ne("KeyframeTrack: Track is empty.",this),e=!1);let o=null;for(let a=0;a!==s;a++){let c=i[a];if(typeof c=="number"&&isNaN(c)){Ne("KeyframeTrack: Time is not a valid number.",this,a,c),e=!1;break}if(o!==null&&o>c){Ne("KeyframeTrack: Out of order keys.",this,a,c,o),e=!1;break}o=c}if(r!==void 0&&Dw(r))for(let a=0,c=r.length;a!==c;++a){let l=r[a];if(isNaN(l)){Ne("KeyframeTrack: Value is not a valid number.",this,a,l),e=!1;break}}return e}optimize(){let e=this.times.slice(),t=this.values.slice(),i=this.getValueSize(),r=this.getInterpolation()===Cu,s=e.length-1,o=1;for(let a=1;a<s;++a){let c=!1,l=e[a],u=e[a+1];if(l!==u&&(a!==1||l!==e[0]))if(r)c=!0;else{let f=a*i,d=f-i,h=f+i;for(let g=0;g!==i;++g){let x=t[f+g];if(x!==t[d+g]||x!==t[h+g]){c=!0;break}}}if(c){if(a!==o){e[o]=e[a];let f=a*i,d=o*i;for(let h=0;h!==i;++h)t[d+h]=t[f+h]}++o}}if(s>0){e[o]=e[s];for(let a=s*i,c=o*i,l=0;l!==i;++l)t[c+l]=t[a+l];++o}return o!==e.length?(this.times=e.slice(0,o),this.values=t.slice(0,o*i)):(this.times=e,this.values=t),this}clone(){let e=this.times.slice(),t=this.values.slice(),i=this.constructor,r=new i(this.name,e,t);return r.createInterpolant=this.createInterpolant,r}};kn.prototype.ValueTypeName="";kn.prototype.TimeBufferType=Float32Array;kn.prototype.ValueBufferType=Float32Array;kn.prototype.DefaultInterpolation=Uu;var kr=class extends kn{constructor(e,t,i){super(e,t,i)}};kr.prototype.ValueTypeName="bool";kr.prototype.ValueBufferType=Array;kr.prototype.DefaultInterpolation=ka;kr.prototype.InterpolantFactoryMethodLinear=void 0;kr.prototype.InterpolantFactoryMethodSmooth=void 0;var Ku=class extends kn{constructor(e,t,i,r){super(e,t,i,r)}};Ku.prototype.ValueTypeName="color";var Qu=class extends kn{constructor(e,t,i,r){super(e,t,i,r)}};Qu.prototype.ValueTypeName="number";var ed=class extends Or{constructor(e,t,i,r){super(e,t,i,r)}interpolate_(e,t,i,r){let s=this.resultBuffer,o=this.sampleValues,a=this.valueSize,c=(i-t)/(r-t),l=e*a;for(let u=l+a;l!==u;l+=4)Di.slerpFlat(s,0,o,l-a,o,l,c);return s}},ec=class extends kn{constructor(e,t,i,r){super(e,t,i,r)}InterpolantFactoryMethodLinear(e){return new ed(this.times,this.values,this.getValueSize(),e)}};ec.prototype.ValueTypeName="quaternion";ec.prototype.InterpolantFactoryMethodSmooth=void 0;var Ur=class extends kn{constructor(e,t,i){super(e,t,i)}};Ur.prototype.ValueTypeName="string";Ur.prototype.ValueBufferType=Array;Ur.prototype.DefaultInterpolation=ka;Ur.prototype.InterpolantFactoryMethodLinear=void 0;Ur.prototype.InterpolantFactoryMethodSmooth=void 0;var td=class extends kn{constructor(e,t,i,r){super(e,t,i,r)}};td.prototype.ValueTypeName="vector";var nd=class extends cr{constructor(e,t=1){super(),this.isLight=!0,this.type="Light",this.color=new Xe(e),this.intensity=t}dispose(){this.dispatchEvent({type:"dispose"})}copy(e,t){return super.copy(e,t),this.color.copy(e.color),this.intensity=e.intensity,this}toJSON(e){let t=super.toJSON(e);return t.object.color=this.color.getHex(),t.object.intensity=this.intensity,t}};var Su=new k,Tu=new Di,Si=new k,tc=class extends cr{constructor(){super(),this.isCamera=!0,this.type="Camera",this.matrixWorldInverse=new Bt,this.projectionMatrix=new Bt,this.projectionMatrixInverse=new Bt,this.coordinateSystem=hi,this._reversedDepth=!1}get reversedDepth(){return this._reversedDepth}copy(e,t){return super.copy(e,t),this.matrixWorldInverse.copy(e.matrixWorldInverse),this.projectionMatrix.copy(e.projectionMatrix),this.projectionMatrixInverse.copy(e.projectionMatrixInverse),this.coordinateSystem=e.coordinateSystem,this}getWorldDirection(e){return super.getWorldDirection(e).negate()}updateMatrixWorld(e){super.updateMatrixWorld(e),this.matrixWorld.decompose(Su,Tu,Si),Si.x===1&&Si.y===1&&Si.z===1?this.matrixWorldInverse.copy(this.matrixWorld).invert():this.matrixWorldInverse.compose(Su,Tu,Si.set(1,1,1)).invert()}updateWorldMatrix(e,t){super.updateWorldMatrix(e,t),this.matrixWorld.decompose(Su,Tu,Si),Si.x===1&&Si.y===1&&Si.z===1?this.matrixWorldInverse.copy(this.matrixWorld).invert():this.matrixWorldInverse.compose(Su,Tu,Si.set(1,1,1)).invert()}clone(){return new this.constructor().copy(this)}},Rr=new k,px=new pt,mx=new pt,hn=class extends tc{constructor(e=50,t=1,i=.1,r=2e3){super(),this.isPerspectiveCamera=!0,this.type="PerspectiveCamera",this.fov=e,this.zoom=1,this.near=i,this.far=r,this.focus=10,this.aspect=t,this.view=null,this.filmGauge=35,this.filmOffset=0,this.updateProjectionMatrix()}copy(e,t){return super.copy(e,t),this.fov=e.fov,this.zoom=e.zoom,this.near=e.near,this.far=e.far,this.focus=e.focus,this.aspect=e.aspect,this.view=e.view===null?null:Object.assign({},e.view),this.filmGauge=e.filmGauge,this.filmOffset=e.filmOffset,this}setFocalLength(e){let t=.5*this.getFilmHeight()/e;this.fov=Bu*2*Math.atan(t),this.updateProjectionMatrix()}getFocalLength(){let e=Math.tan(vm*.5*this.fov);return .5*this.getFilmHeight()/e}getEffectiveFOV(){return Bu*2*Math.atan(Math.tan(vm*.5*this.fov)/this.zoom)}getFilmWidth(){return this.filmGauge*Math.min(this.aspect,1)}getFilmHeight(){return this.filmGauge/Math.max(this.aspect,1)}getViewBounds(e,t,i){Rr.set(-1,-1,.5).applyMatrix4(this.projectionMatrixInverse),t.set(Rr.x,Rr.y).multiplyScalar(-e/Rr.z),Rr.set(1,1,.5).applyMatrix4(this.projectionMatrixInverse),i.set(Rr.x,Rr.y).multiplyScalar(-e/Rr.z)}getViewSize(e,t){return this.getViewBounds(e,px,mx),t.subVectors(mx,px)}setViewOffset(e,t,i,r,s,o){this.aspect=e/t,this.view===null&&(this.view={enabled:!0,fullWidth:1,fullHeight:1,offsetX:0,offsetY:0,width:1,height:1}),this.view.enabled=!0,this.view.fullWidth=e,this.view.fullHeight=t,this.view.offsetX=i,this.view.offsetY=r,this.view.width=s,this.view.height=o,this.updateProjectionMatrix()}clearViewOffset(){this.view!==null&&(this.view.enabled=!1),this.updateProjectionMatrix()}updateProjectionMatrix(){let e=this.near,t=e*Math.tan(vm*.5*this.fov)/this.zoom,i=2*t,r=this.aspect*i,s=-.5*r,o=this.view;if(this.view!==null&&this.view.enabled){let c=o.fullWidth,l=o.fullHeight;s+=o.offsetX*r/c,t-=o.offsetY*i/l,r*=o.width/c,i*=o.height/l}let a=this.filmOffset;a!==0&&(s+=e*a/this.getFilmWidth()),this.projectionMatrix.makePerspective(s,s+r,t,t-i,e,this.far,this.coordinateSystem,this.reversedDepth),this.projectionMatrixInverse.copy(this.projectionMatrix).invert()}toJSON(e){let t=super.toJSON(e);return t.object.fov=this.fov,t.object.zoom=this.zoom,t.object.near=this.near,t.object.far=this.far,t.object.focus=this.focus,t.object.aspect=this.aspect,this.view!==null&&(t.object.view=Object.assign({},this.view)),t.object.filmGauge=this.filmGauge,t.object.filmOffset=this.filmOffset,t}};var nc=class extends tc{constructor(e=-1,t=1,i=1,r=-1,s=.1,o=2e3){super(),this.isOrthographicCamera=!0,this.type="OrthographicCamera",this.zoom=1,this.view=null,this.left=e,this.right=t,this.top=i,this.bottom=r,this.near=s,this.far=o,this.updateProjectionMatrix()}copy(e,t){return super.copy(e,t),this.left=e.left,this.right=e.right,this.top=e.top,this.bottom=e.bottom,this.near=e.near,this.far=e.far,this.zoom=e.zoom,this.view=e.view===null?null:Object.assign({},e.view),this}setViewOffset(e,t,i,r,s,o){this.view===null&&(this.view={enabled:!0,fullWidth:1,fullHeight:1,offsetX:0,offsetY:0,width:1,height:1}),this.view.enabled=!0,this.view.fullWidth=e,this.view.fullHeight=t,this.view.offsetX=i,this.view.offsetY=r,this.view.width=s,this.view.height=o,this.updateProjectionMatrix()}clearViewOffset(){this.view!==null&&(this.view.enabled=!1),this.updateProjectionMatrix()}updateProjectionMatrix(){let e=(this.right-this.left)/(2*this.zoom),t=(this.top-this.bottom)/(2*this.zoom),i=(this.right+this.left)/2,r=(this.top+this.bottom)/2,s=i-e,o=i+e,a=r+t,c=r-t;if(this.view!==null&&this.view.enabled){let l=(this.right-this.left)/this.view.fullWidth/this.zoom,u=(this.top-this.bottom)/this.view.fullHeight/this.zoom;s+=l*this.view.offsetX,o=s+l*this.view.width,a-=u*this.view.offsetY,c=a-u*this.view.height}this.projectionMatrix.makeOrthographic(s,o,a,c,this.near,this.far,this.coordinateSystem,this.reversedDepth),this.projectionMatrixInverse.copy(this.projectionMatrix).invert()}toJSON(e){let t=super.toJSON(e);return t.object.zoom=this.zoom,t.object.left=this.left,t.object.right=this.right,t.object.top=this.top,t.object.bottom=this.bottom,t.object.near=this.near,t.object.far=this.far,this.view!==null&&(t.object.view=Object.assign({},this.view)),t}};var ic=class extends nd{constructor(e,t){super(e,t),this.isAmbientLight=!0,this.type="AmbientLight"}};var Eo=-90,So=1,id=class extends cr{constructor(e,t,i){super(),this.type="CubeCamera",this.renderTarget=i,this.coordinateSystem=null,this.activeMipmapLevel=0;let r=new hn(Eo,So,e,t);r.layers=this.layers,this.add(r);let s=new hn(Eo,So,e,t);s.layers=this.layers,this.add(s);let o=new hn(Eo,So,e,t);o.layers=this.layers,this.add(o);let a=new hn(Eo,So,e,t);a.layers=this.layers,this.add(a);let c=new hn(Eo,So,e,t);c.layers=this.layers,this.add(c);let l=new hn(Eo,So,e,t);l.layers=this.layers,this.add(l)}updateCoordinateSystem(){let e=this.coordinateSystem,t=this.children.concat(),[i,r,s,o,a,c]=t;for(let l of t)this.remove(l);if(e===hi)i.up.set(0,1,0),i.lookAt(1,0,0),r.up.set(0,1,0),r.lookAt(-1,0,0),s.up.set(0,0,-1),s.lookAt(0,1,0),o.up.set(0,0,1),o.lookAt(0,-1,0),a.up.set(0,1,0),a.lookAt(0,0,1),c.up.set(0,1,0),c.lookAt(0,0,-1);else if(e===Ba)i.up.set(0,-1,0),i.lookAt(-1,0,0),r.up.set(0,-1,0),r.lookAt(1,0,0),s.up.set(0,0,1),s.lookAt(0,1,0),o.up.set(0,0,-1),o.lookAt(0,-1,0),a.up.set(0,-1,0),a.lookAt(0,0,1),c.up.set(0,-1,0),c.lookAt(0,0,-1);else throw new Error("THREE.CubeCamera.updateCoordinateSystem(): Invalid coordinate system: "+e);for(let l of t)this.add(l),l.updateMatrixWorld()}update(e,t){this.parent===null&&this.updateMatrixWorld();let{renderTarget:i,activeMipmapLevel:r}=this;this.coordinateSystem!==e.coordinateSystem&&(this.coordinateSystem=e.coordinateSystem,this.updateCoordinateSystem());let[s,o,a,c,l,u]=this.children,f=e.getRenderTarget(),d=e.getActiveCubeFace(),h=e.getActiveMipmapLevel(),g=e.xr.enabled;e.xr.enabled=!1;let x=i.texture.generateMipmaps;i.texture.generateMipmaps=!1;let m=!1;e.isWebGLRenderer===!0?m=e.state.buffers.depth.getReversed():m=e.reversedDepthBuffer,e.setRenderTarget(i,0,r),m&&e.autoClear===!1&&e.clearDepth(),e.render(t,s),e.setRenderTarget(i,1,r),m&&e.autoClear===!1&&e.clearDepth(),e.render(t,o),e.setRenderTarget(i,2,r),m&&e.autoClear===!1&&e.clearDepth(),e.render(t,a),e.setRenderTarget(i,3,r),m&&e.autoClear===!1&&e.clearDepth(),e.render(t,c),e.setRenderTarget(i,4,r),m&&e.autoClear===!1&&e.clearDepth(),e.render(t,l),i.texture.generateMipmaps=x,e.setRenderTarget(i,5,r),m&&e.autoClear===!1&&e.clearDepth(),e.render(t,u),e.setRenderTarget(f,d,h),e.xr.enabled=g,i.texture.needsPMREMUpdate=!0}},rd=class extends hn{constructor(e=[]){super(),this.isArrayCamera=!0,this.isMultiViewCamera=!1,this.cameras=e}};var vg="\\[\\]\\.:\\/",Jw=new RegExp("["+vg+"]","g"),yg="[^"+vg+"]",Kw="[^"+vg.replace("\\.","")+"]",Qw=/((?:WC+[\/:])*)/.source.replace("WC",yg),eD=/(WCOD+)?/.source.replace("WCOD",Kw),tD=/(?:\.(WC+)(?:\[(.+)\])?)?/.source.replace("WC",yg),nD=/\.(WC+)(?:\[(.+)\])?/.source.replace("WC",yg),iD=new RegExp("^"+Qw+eD+tD+nD+"$"),rD=["material","materials","bones","map"],Xm=class{constructor(e,t,i){let r=i||Pt.parseTrackName(t);this._targetGroup=e,this._bindings=e.subscribe_(t,r)}getValue(e,t){this.bind();let i=this._targetGroup.nCachedObjects_,r=this._bindings[i];r!==void 0&&r.getValue(e,t)}setValue(e,t){let i=this._bindings;for(let r=this._targetGroup.nCachedObjects_,s=i.length;r!==s;++r)i[r].setValue(e,t)}bind(){let e=this._bindings;for(let t=this._targetGroup.nCachedObjects_,i=e.length;t!==i;++t)e[t].bind()}unbind(){let e=this._bindings;for(let t=this._targetGroup.nCachedObjects_,i=e.length;t!==i;++t)e[t].unbind()}},Pt=(()=>{class n{constructor(t,i,r){this.path=i,this.parsedPath=r||n.parseTrackName(i),this.node=n.findNode(t,this.parsedPath.nodeName),this.rootNode=t,this.getValue=this._getValue_unbound,this.setValue=this._setValue_unbound}static create(t,i,r){return t&&t.isAnimationObjectGroup?new n.Composite(t,i,r):new n(t,i,r)}static sanitizeNodeName(t){return t.replace(/\s/g,"_").replace(Jw,"")}static parseTrackName(t){let i=iD.exec(t);if(i===null)throw new Error("PropertyBinding: Cannot parse trackName: "+t);let r={nodeName:i[2],objectName:i[3],objectIndex:i[4],propertyName:i[5],propertyIndex:i[6]},s=r.nodeName&&r.nodeName.lastIndexOf(".");if(s!==void 0&&s!==-1){let o=r.nodeName.substring(s+1);rD.indexOf(o)!==-1&&(r.nodeName=r.nodeName.substring(0,s),r.objectName=o)}if(r.propertyName===null||r.propertyName.length===0)throw new Error("PropertyBinding: can not parse propertyName from trackName: "+t);return r}static findNode(t,i){if(i===void 0||i===""||i==="."||i===-1||i===t.name||i===t.uuid)return t;if(t.skeleton){let r=t.skeleton.getBoneByName(i);if(r!==void 0)return r}if(t.children){let r=function(o){for(let a=0;a<o.length;a++){let c=o[a];if(c.name===i||c.uuid===i)return c;let l=r(c.children);if(l)return l}return null},s=r(t.children);if(s)return s}return null}_getValue_unavailable(){}_setValue_unavailable(){}_getValue_direct(t,i){t[i]=this.targetObject[this.propertyName]}_getValue_array(t,i){let r=this.resolvedProperty;for(let s=0,o=r.length;s!==o;++s)t[i++]=r[s]}_getValue_arrayElement(t,i){t[i]=this.resolvedProperty[this.propertyIndex]}_getValue_toArray(t,i){this.resolvedProperty.toArray(t,i)}_setValue_direct(t,i){this.targetObject[this.propertyName]=t[i]}_setValue_direct_setNeedsUpdate(t,i){this.targetObject[this.propertyName]=t[i],this.targetObject.needsUpdate=!0}_setValue_direct_setMatrixWorldNeedsUpdate(t,i){this.targetObject[this.propertyName]=t[i],this.targetObject.matrixWorldNeedsUpdate=!0}_setValue_array(t,i){let r=this.resolvedProperty;for(let s=0,o=r.length;s!==o;++s)r[s]=t[i++]}_setValue_array_setNeedsUpdate(t,i){let r=this.resolvedProperty;for(let s=0,o=r.length;s!==o;++s)r[s]=t[i++];this.targetObject.needsUpdate=!0}_setValue_array_setMatrixWorldNeedsUpdate(t,i){let r=this.resolvedProperty;for(let s=0,o=r.length;s!==o;++s)r[s]=t[i++];this.targetObject.matrixWorldNeedsUpdate=!0}_setValue_arrayElement(t,i){this.resolvedProperty[this.propertyIndex]=t[i]}_setValue_arrayElement_setNeedsUpdate(t,i){this.resolvedProperty[this.propertyIndex]=t[i],this.targetObject.needsUpdate=!0}_setValue_arrayElement_setMatrixWorldNeedsUpdate(t,i){this.resolvedProperty[this.propertyIndex]=t[i],this.targetObject.matrixWorldNeedsUpdate=!0}_setValue_fromArray(t,i){this.resolvedProperty.fromArray(t,i)}_setValue_fromArray_setNeedsUpdate(t,i){this.resolvedProperty.fromArray(t,i),this.targetObject.needsUpdate=!0}_setValue_fromArray_setMatrixWorldNeedsUpdate(t,i){this.resolvedProperty.fromArray(t,i),this.targetObject.matrixWorldNeedsUpdate=!0}_getValue_unbound(t,i){this.bind(),this.getValue(t,i)}_setValue_unbound(t,i){this.bind(),this.setValue(t,i)}bind(){let t=this.node,i=this.parsedPath,r=i.objectName,s=i.propertyName,o=i.propertyIndex;if(t||(t=n.findNode(this.rootNode,i.nodeName),this.node=t),this.getValue=this._getValue_unavailable,this.setValue=this._setValue_unavailable,!t){Le("PropertyBinding: No target node found for track: "+this.path+".");return}if(r){let u=i.objectIndex;switch(r){case"materials":if(!t.material){Ne("PropertyBinding: Can not bind to material as node does not have a material.",this);return}if(!t.material.materials){Ne("PropertyBinding: Can not bind to material.materials as node.material does not have a materials array.",this);return}t=t.material.materials;break;case"bones":if(!t.skeleton){Ne("PropertyBinding: Can not bind to bones as node does not have a skeleton.",this);return}t=t.skeleton.bones;for(let f=0;f<t.length;f++)if(t[f].name===u){u=f;break}break;case"map":if("map"in t){t=t.map;break}if(!t.material){Ne("PropertyBinding: Can not bind to material as node does not have a material.",this);return}if(!t.material.map){Ne("PropertyBinding: Can not bind to material.map as node.material does not have a map.",this);return}t=t.material.map;break;default:if(t[r]===void 0){Ne("PropertyBinding: Can not bind to objectName of node undefined.",this);return}t=t[r]}if(u!==void 0){if(t[u]===void 0){Ne("PropertyBinding: Trying to bind to objectIndex of objectName, but is undefined.",this,t);return}t=t[u]}}let a=t[s];if(a===void 0){let u=i.nodeName;Ne("PropertyBinding: Trying to update property for track: "+u+"."+s+" but it wasn't found.",t);return}let c=this.Versioning.None;this.targetObject=t,t.isMaterial===!0?c=this.Versioning.NeedsUpdate:t.isObject3D===!0&&(c=this.Versioning.MatrixWorldNeedsUpdate);let l=this.BindingType.Direct;if(o!==void 0){if(s==="morphTargetInfluences"){if(!t.geometry){Ne("PropertyBinding: Can not bind to morphTargetInfluences because node does not have a geometry.",this);return}if(!t.geometry.morphAttributes){Ne("PropertyBinding: Can not bind to morphTargetInfluences because node does not have a geometry.morphAttributes.",this);return}t.morphTargetDictionary[o]!==void 0&&(o=t.morphTargetDictionary[o])}l=this.BindingType.ArrayElement,this.resolvedProperty=a,this.propertyIndex=o}else a.fromArray!==void 0&&a.toArray!==void 0?(l=this.BindingType.HasFromToArray,this.resolvedProperty=a):Array.isArray(a)?(l=this.BindingType.EntireArray,this.resolvedProperty=a):this.propertyName=s;this.getValue=this.GetterByBindingType[l],this.setValue=this.SetterByBindingTypeAndVersioning[l][c]}unbind(){this.node=null,this.getValue=this._getValue_unbound,this.setValue=this._setValue_unbound}}return n.Composite=Xm,n})();Pt.prototype.BindingType={Direct:0,EntireArray:1,ArrayElement:2,HasFromToArray:3};Pt.prototype.Versioning={None:0,NeedsUpdate:1,MatrixWorldNeedsUpdate:2};Pt.prototype.GetterByBindingType=[Pt.prototype._getValue_direct,Pt.prototype._getValue_array,Pt.prototype._getValue_arrayElement,Pt.prototype._getValue_toArray];Pt.prototype.SetterByBindingTypeAndVersioning=[[Pt.prototype._setValue_direct,Pt.prototype._setValue_direct_setNeedsUpdate,Pt.prototype._setValue_direct_setMatrixWorldNeedsUpdate],[Pt.prototype._setValue_array,Pt.prototype._setValue_array_setNeedsUpdate,Pt.prototype._setValue_array_setMatrixWorldNeedsUpdate],[Pt.prototype._setValue_arrayElement,Pt.prototype._setValue_arrayElement_setNeedsUpdate,Pt.prototype._setValue_arrayElement_setMatrixWorldNeedsUpdate],[Pt.prototype._setValue_fromArray,Pt.prototype._setValue_fromArray_setNeedsUpdate,Pt.prototype._setValue_fromArray_setMatrixWorldNeedsUpdate]];var Nz=new Float32Array(1);function _g(n,e,t,i){let r=sD(i);switch(t){case dg:return n*e;case hg:return n*e/r.components*r.byteLength;case dd:return n*e/r.components*r.byteLength;case ws:return n*e*2/r.components*r.byteLength;case fd:return n*e*2/r.components*r.byteLength;case fg:return n*e*3/r.components*r.byteLength;case Jn:return n*e*4/r.components*r.byteLength;case hd:return n*e*4/r.components*r.byteLength;case ac:case cc:return Math.floor((n+3)/4)*Math.floor((e+3)/4)*8;case lc:case uc:return Math.floor((n+3)/4)*Math.floor((e+3)/4)*16;case md:case vd:return Math.max(n,16)*Math.max(e,8)/4;case pd:case gd:return Math.max(n,8)*Math.max(e,8)/2;case yd:case _d:case Md:case bd:return Math.floor((n+3)/4)*Math.floor((e+3)/4)*8;case xd:case Ed:case Sd:return Math.floor((n+3)/4)*Math.floor((e+3)/4)*16;case Td:return Math.floor((n+3)/4)*Math.floor((e+3)/4)*16;case Cd:return Math.floor((n+4)/5)*Math.floor((e+3)/4)*16;case wd:return Math.floor((n+4)/5)*Math.floor((e+4)/5)*16;case Dd:return Math.floor((n+5)/6)*Math.floor((e+4)/5)*16;case Id:return Math.floor((n+5)/6)*Math.floor((e+5)/6)*16;case Ad:return Math.floor((n+7)/8)*Math.floor((e+4)/5)*16;case Rd:return Math.floor((n+7)/8)*Math.floor((e+5)/6)*16;case Nd:return Math.floor((n+7)/8)*Math.floor((e+7)/8)*16;case Pd:return Math.floor((n+9)/10)*Math.floor((e+4)/5)*16;case Ld:return Math.floor((n+9)/10)*Math.floor((e+5)/6)*16;case Fd:return Math.floor((n+9)/10)*Math.floor((e+7)/8)*16;case Od:return Math.floor((n+9)/10)*Math.floor((e+9)/10)*16;case kd:return Math.floor((n+11)/12)*Math.floor((e+9)/10)*16;case Ud:return Math.floor((n+11)/12)*Math.floor((e+11)/12)*16;case Bd:case Vd:case Hd:return Math.ceil(n/4)*Math.ceil(e/4)*16;case zd:case Gd:return Math.ceil(n/4)*Math.ceil(e/4)*8;case Wd:case jd:return Math.ceil(n/4)*Math.ceil(e/4)*16}throw new Error(`Unable to determine texture byte length for ${t} format.`)}function sD(n){switch(n){case Un:case ag:return{byteLength:1,components:1};case No:case cg:case Ni:return{byteLength:2,components:1};case ld:case ud:return{byteLength:2,components:4};case mi:case cd:case gi:return{byteLength:4,components:1};case lg:case ug:return{byteLength:4,components:3}}throw new Error(`Unknown texture type ${n}.`)}typeof __THREE_DEVTOOLS__<"u"&&__THREE_DEVTOOLS__.dispatchEvent(new CustomEvent("register",{detail:{revision:"183"}}));typeof window<"u"&&(window.__THREE__?Le("WARNING: Multiple instances of Three.js being imported."):window.__THREE__="183");function EM(){let n=null,e=!1,t=null,i=null;function r(s,o){t(s,o),i=n.requestAnimationFrame(r)}return{start:function(){e!==!0&&t!==null&&(i=n.requestAnimationFrame(r),e=!0)},stop:function(){n.cancelAnimationFrame(i),e=!1},setAnimationLoop:function(s){t=s},setContext:function(s){n=s}}}function aD(n){let e=new WeakMap;function t(a,c){let l=a.array,u=a.usage,f=l.byteLength,d=n.createBuffer();n.bindBuffer(c,d),n.bufferData(c,l,u),a.onUploadCallback();let h;if(l instanceof Float32Array)h=n.FLOAT;else if(typeof Float16Array<"u"&&l instanceof Float16Array)h=n.HALF_FLOAT;else if(l instanceof Uint16Array)a.isFloat16BufferAttribute?h=n.HALF_FLOAT:h=n.UNSIGNED_SHORT;else if(l instanceof Int16Array)h=n.SHORT;else if(l instanceof Uint32Array)h=n.UNSIGNED_INT;else if(l instanceof Int32Array)h=n.INT;else if(l instanceof Int8Array)h=n.BYTE;else if(l instanceof Uint8Array)h=n.UNSIGNED_BYTE;else if(l instanceof Uint8ClampedArray)h=n.UNSIGNED_BYTE;else throw new Error("THREE.WebGLAttributes: Unsupported buffer data format: "+l);return{buffer:d,type:h,bytesPerElement:l.BYTES_PER_ELEMENT,version:a.version,size:f}}function i(a,c,l){let u=c.array,f=c.updateRanges;if(n.bindBuffer(l,a),f.length===0)n.bufferSubData(l,0,u);else{f.sort((h,g)=>h.start-g.start);let d=0;for(let h=1;h<f.length;h++){let g=f[d],x=f[h];x.start<=g.start+g.count+1?g.count=Math.max(g.count,x.start+x.count-g.start):(++d,f[d]=x)}f.length=d+1;for(let h=0,g=f.length;h<g;h++){let x=f[h];n.bufferSubData(l,x.start*u.BYTES_PER_ELEMENT,u,x.start,x.count)}c.clearUpdateRanges()}c.onUploadCallback()}function r(a){return a.isInterleavedBufferAttribute&&(a=a.data),e.get(a)}function s(a){a.isInterleavedBufferAttribute&&(a=a.data);let c=e.get(a);c&&(n.deleteBuffer(c.buffer),e.delete(a))}function o(a,c){if(a.isInterleavedBufferAttribute&&(a=a.data),a.isGLBufferAttribute){let u=e.get(a);(!u||u.version<a.version)&&e.set(a,{buffer:a.buffer,type:a.type,bytesPerElement:a.elementSize,version:a.version});return}let l=e.get(a);if(l===void 0)e.set(a,t(a,c));else if(l.version<a.version){if(l.size!==a.array.byteLength)throw new Error("THREE.WebGLAttributes: The size of the buffer attribute's array buffer does not match the original size. Resizing buffer attributes is not supported.");i(l.buffer,a,c),l.version=a.version}}return{get:r,remove:s,update:o}}var cD=`#ifdef USE_ALPHAHASH
	if ( diffuseColor.a < getAlphaHashThreshold( vPosition ) ) discard;
#endif`,lD=`#ifdef USE_ALPHAHASH
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
#endif`,uD=`#ifdef USE_ALPHAMAP
	diffuseColor.a *= texture2D( alphaMap, vAlphaMapUv ).g;
#endif`,dD=`#ifdef USE_ALPHAMAP
	uniform sampler2D alphaMap;
#endif`,fD=`#ifdef USE_ALPHATEST
	#ifdef ALPHA_TO_COVERAGE
	diffuseColor.a = smoothstep( alphaTest, alphaTest + fwidth( diffuseColor.a ), diffuseColor.a );
	if ( diffuseColor.a == 0.0 ) discard;
	#else
	if ( diffuseColor.a < alphaTest ) discard;
	#endif
#endif`,hD=`#ifdef USE_ALPHATEST
	uniform float alphaTest;
#endif`,pD=`#ifdef USE_AOMAP
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
#endif`,mD=`#ifdef USE_AOMAP
	uniform sampler2D aoMap;
	uniform float aoMapIntensity;
#endif`,gD=`#ifdef USE_BATCHING
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
#endif`,vD=`#ifdef USE_BATCHING
	mat4 batchingMatrix = getBatchingMatrix( getIndirectIndex( gl_DrawID ) );
#endif`,yD=`vec3 transformed = vec3( position );
#ifdef USE_ALPHAHASH
	vPosition = vec3( position );
#endif`,_D=`vec3 objectNormal = vec3( normal );
#ifdef USE_TANGENT
	vec3 objectTangent = vec3( tangent.xyz );
#endif`,xD=`float G_BlinnPhong_Implicit( ) {
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
} // validated`,MD=`#ifdef USE_IRIDESCENCE
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
#endif`,bD=`#ifdef USE_BUMPMAP
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
#endif`,ED=`#if NUM_CLIPPING_PLANES > 0
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
#endif`,SD=`#if NUM_CLIPPING_PLANES > 0
	varying vec3 vClipPosition;
	uniform vec4 clippingPlanes[ NUM_CLIPPING_PLANES ];
#endif`,TD=`#if NUM_CLIPPING_PLANES > 0
	varying vec3 vClipPosition;
#endif`,CD=`#if NUM_CLIPPING_PLANES > 0
	vClipPosition = - mvPosition.xyz;
#endif`,wD=`#if defined( USE_COLOR ) || defined( USE_COLOR_ALPHA )
	diffuseColor *= vColor;
#endif`,DD=`#if defined( USE_COLOR ) || defined( USE_COLOR_ALPHA )
	varying vec4 vColor;
#endif`,ID=`#if defined( USE_COLOR ) || defined( USE_COLOR_ALPHA ) || defined( USE_INSTANCING_COLOR ) || defined( USE_BATCHING_COLOR )
	varying vec4 vColor;
#endif`,AD=`#if defined( USE_COLOR ) || defined( USE_COLOR_ALPHA ) || defined( USE_INSTANCING_COLOR ) || defined( USE_BATCHING_COLOR )
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
#endif`,RD=`#define PI 3.141592653589793
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
} // validated`,ND=`#ifdef ENVMAP_TYPE_CUBE_UV
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
#endif`,PD=`vec3 transformedNormal = objectNormal;
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
#endif`,LD=`#ifdef USE_DISPLACEMENTMAP
	uniform sampler2D displacementMap;
	uniform float displacementScale;
	uniform float displacementBias;
#endif`,FD=`#ifdef USE_DISPLACEMENTMAP
	transformed += normalize( objectNormal ) * ( texture2D( displacementMap, vDisplacementMapUv ).x * displacementScale + displacementBias );
#endif`,OD=`#ifdef USE_EMISSIVEMAP
	vec4 emissiveColor = texture2D( emissiveMap, vEmissiveMapUv );
	#ifdef DECODE_VIDEO_TEXTURE_EMISSIVE
		emissiveColor = sRGBTransferEOTF( emissiveColor );
	#endif
	totalEmissiveRadiance *= emissiveColor.rgb;
#endif`,kD=`#ifdef USE_EMISSIVEMAP
	uniform sampler2D emissiveMap;
#endif`,UD="gl_FragColor = linearToOutputTexel( gl_FragColor );",BD=`vec4 LinearTransferOETF( in vec4 value ) {
	return value;
}
vec4 sRGBTransferEOTF( in vec4 value ) {
	return vec4( mix( pow( value.rgb * 0.9478672986 + vec3( 0.0521327014 ), vec3( 2.4 ) ), value.rgb * 0.0773993808, vec3( lessThanEqual( value.rgb, vec3( 0.04045 ) ) ) ), value.a );
}
vec4 sRGBTransferOETF( in vec4 value ) {
	return vec4( mix( pow( value.rgb, vec3( 0.41666 ) ) * 1.055 - vec3( 0.055 ), value.rgb * 12.92, vec3( lessThanEqual( value.rgb, vec3( 0.0031308 ) ) ) ), value.a );
}`,VD=`#ifdef USE_ENVMAP
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
#endif`,HD=`#ifdef USE_ENVMAP
	uniform float envMapIntensity;
	uniform float flipEnvMap;
	uniform mat3 envMapRotation;
	#ifdef ENVMAP_TYPE_CUBE
		uniform samplerCube envMap;
	#else
		uniform sampler2D envMap;
	#endif
#endif`,zD=`#ifdef USE_ENVMAP
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
#endif`,GD=`#ifdef USE_ENVMAP
	#if defined( USE_BUMPMAP ) || defined( USE_NORMALMAP ) || defined( PHONG ) || defined( LAMBERT )
		#define ENV_WORLDPOS
	#endif
	#ifdef ENV_WORLDPOS
		
		varying vec3 vWorldPosition;
	#else
		varying vec3 vReflect;
		uniform float refractionRatio;
	#endif
#endif`,WD=`#ifdef USE_ENVMAP
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
#endif`,jD=`#ifdef USE_FOG
	vFogDepth = - mvPosition.z;
#endif`,$D=`#ifdef USE_FOG
	varying float vFogDepth;
#endif`,qD=`#ifdef USE_FOG
	#ifdef FOG_EXP2
		float fogFactor = 1.0 - exp( - fogDensity * fogDensity * vFogDepth * vFogDepth );
	#else
		float fogFactor = smoothstep( fogNear, fogFar, vFogDepth );
	#endif
	gl_FragColor.rgb = mix( gl_FragColor.rgb, fogColor, fogFactor );
#endif`,XD=`#ifdef USE_FOG
	uniform vec3 fogColor;
	varying float vFogDepth;
	#ifdef FOG_EXP2
		uniform float fogDensity;
	#else
		uniform float fogNear;
		uniform float fogFar;
	#endif
#endif`,YD=`#ifdef USE_GRADIENTMAP
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
}`,ZD=`#ifdef USE_LIGHTMAP
	uniform sampler2D lightMap;
	uniform float lightMapIntensity;
#endif`,JD=`LambertMaterial material;
material.diffuseColor = diffuseColor.rgb;
material.specularStrength = specularStrength;`,KD=`varying vec3 vViewPosition;
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
#define RE_IndirectDiffuse		RE_IndirectDiffuse_Lambert`,QD=`uniform bool receiveShadow;
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
#endif`,eI=`#ifdef USE_ENVMAP
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
#endif`,tI=`ToonMaterial material;
material.diffuseColor = diffuseColor.rgb;`,nI=`varying vec3 vViewPosition;
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
#define RE_IndirectDiffuse		RE_IndirectDiffuse_Toon`,iI=`BlinnPhongMaterial material;
material.diffuseColor = diffuseColor.rgb;
material.specularColor = specular;
material.specularShininess = shininess;
material.specularStrength = specularStrength;`,rI=`varying vec3 vViewPosition;
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
#define RE_IndirectDiffuse		RE_IndirectDiffuse_BlinnPhong`,sI=`PhysicalMaterial material;
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
#endif`,oI=`uniform sampler2D dfgLUT;
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
}`,aI=`
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
#endif`,cI=`#if defined( RE_IndirectDiffuse )
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
#endif`,lI=`#if defined( RE_IndirectDiffuse )
	#if defined( LAMBERT ) || defined( PHONG )
		irradiance += iblIrradiance;
	#endif
	RE_IndirectDiffuse( irradiance, geometryPosition, geometryNormal, geometryViewDir, geometryClearcoatNormal, material, reflectedLight );
#endif
#if defined( RE_IndirectSpecular )
	RE_IndirectSpecular( radiance, iblIrradiance, clearcoatRadiance, geometryPosition, geometryNormal, geometryViewDir, geometryClearcoatNormal, material, reflectedLight );
#endif`,uI=`#if defined( USE_LOGARITHMIC_DEPTH_BUFFER )
	gl_FragDepth = vIsPerspective == 0.0 ? gl_FragCoord.z : log2( vFragDepth ) * logDepthBufFC * 0.5;
#endif`,dI=`#if defined( USE_LOGARITHMIC_DEPTH_BUFFER )
	uniform float logDepthBufFC;
	varying float vFragDepth;
	varying float vIsPerspective;
#endif`,fI=`#ifdef USE_LOGARITHMIC_DEPTH_BUFFER
	varying float vFragDepth;
	varying float vIsPerspective;
#endif`,hI=`#ifdef USE_LOGARITHMIC_DEPTH_BUFFER
	vFragDepth = 1.0 + gl_Position.w;
	vIsPerspective = float( isPerspectiveMatrix( projectionMatrix ) );
#endif`,pI=`#ifdef USE_MAP
	vec4 sampledDiffuseColor = texture2D( map, vMapUv );
	#ifdef DECODE_VIDEO_TEXTURE
		sampledDiffuseColor = sRGBTransferEOTF( sampledDiffuseColor );
	#endif
	diffuseColor *= sampledDiffuseColor;
#endif`,mI=`#ifdef USE_MAP
	uniform sampler2D map;
#endif`,gI=`#if defined( USE_MAP ) || defined( USE_ALPHAMAP )
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
#endif`,vI=`#if defined( USE_POINTS_UV )
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
#endif`,yI=`float metalnessFactor = metalness;
#ifdef USE_METALNESSMAP
	vec4 texelMetalness = texture2D( metalnessMap, vMetalnessMapUv );
	metalnessFactor *= texelMetalness.b;
#endif`,_I=`#ifdef USE_METALNESSMAP
	uniform sampler2D metalnessMap;
#endif`,xI=`#ifdef USE_INSTANCING_MORPH
	float morphTargetInfluences[ MORPHTARGETS_COUNT ];
	float morphTargetBaseInfluence = texelFetch( morphTexture, ivec2( 0, gl_InstanceID ), 0 ).r;
	for ( int i = 0; i < MORPHTARGETS_COUNT; i ++ ) {
		morphTargetInfluences[i] =  texelFetch( morphTexture, ivec2( i + 1, gl_InstanceID ), 0 ).r;
	}
#endif`,MI=`#if defined( USE_MORPHCOLORS )
	vColor *= morphTargetBaseInfluence;
	for ( int i = 0; i < MORPHTARGETS_COUNT; i ++ ) {
		#if defined( USE_COLOR_ALPHA )
			if ( morphTargetInfluences[ i ] != 0.0 ) vColor += getMorph( gl_VertexID, i, 2 ) * morphTargetInfluences[ i ];
		#elif defined( USE_COLOR )
			if ( morphTargetInfluences[ i ] != 0.0 ) vColor += getMorph( gl_VertexID, i, 2 ).rgb * morphTargetInfluences[ i ];
		#endif
	}
#endif`,bI=`#ifdef USE_MORPHNORMALS
	objectNormal *= morphTargetBaseInfluence;
	for ( int i = 0; i < MORPHTARGETS_COUNT; i ++ ) {
		if ( morphTargetInfluences[ i ] != 0.0 ) objectNormal += getMorph( gl_VertexID, i, 1 ).xyz * morphTargetInfluences[ i ];
	}
#endif`,EI=`#ifdef USE_MORPHTARGETS
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
#endif`,SI=`#ifdef USE_MORPHTARGETS
	transformed *= morphTargetBaseInfluence;
	for ( int i = 0; i < MORPHTARGETS_COUNT; i ++ ) {
		if ( morphTargetInfluences[ i ] != 0.0 ) transformed += getMorph( gl_VertexID, i, 0 ).xyz * morphTargetInfluences[ i ];
	}
#endif`,TI=`float faceDirection = gl_FrontFacing ? 1.0 : - 1.0;
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
vec3 nonPerturbedNormal = normal;`,CI=`#ifdef USE_NORMALMAP_OBJECTSPACE
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
#endif`,wI=`#ifndef FLAT_SHADED
	varying vec3 vNormal;
	#ifdef USE_TANGENT
		varying vec3 vTangent;
		varying vec3 vBitangent;
	#endif
#endif`,DI=`#ifndef FLAT_SHADED
	varying vec3 vNormal;
	#ifdef USE_TANGENT
		varying vec3 vTangent;
		varying vec3 vBitangent;
	#endif
#endif`,II=`#ifndef FLAT_SHADED
	vNormal = normalize( transformedNormal );
	#ifdef USE_TANGENT
		vTangent = normalize( transformedTangent );
		vBitangent = normalize( cross( vNormal, vTangent ) * tangent.w );
	#endif
#endif`,AI=`#ifdef USE_NORMALMAP
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
#endif`,RI=`#ifdef USE_CLEARCOAT
	vec3 clearcoatNormal = nonPerturbedNormal;
#endif`,NI=`#ifdef USE_CLEARCOAT_NORMALMAP
	vec3 clearcoatMapN = texture2D( clearcoatNormalMap, vClearcoatNormalMapUv ).xyz * 2.0 - 1.0;
	clearcoatMapN.xy *= clearcoatNormalScale;
	clearcoatNormal = normalize( tbn2 * clearcoatMapN );
#endif`,PI=`#ifdef USE_CLEARCOATMAP
	uniform sampler2D clearcoatMap;
#endif
#ifdef USE_CLEARCOAT_NORMALMAP
	uniform sampler2D clearcoatNormalMap;
	uniform vec2 clearcoatNormalScale;
#endif
#ifdef USE_CLEARCOAT_ROUGHNESSMAP
	uniform sampler2D clearcoatRoughnessMap;
#endif`,LI=`#ifdef USE_IRIDESCENCEMAP
	uniform sampler2D iridescenceMap;
#endif
#ifdef USE_IRIDESCENCE_THICKNESSMAP
	uniform sampler2D iridescenceThicknessMap;
#endif`,FI=`#ifdef OPAQUE
diffuseColor.a = 1.0;
#endif
#ifdef USE_TRANSMISSION
diffuseColor.a *= material.transmissionAlpha;
#endif
gl_FragColor = vec4( outgoingLight, diffuseColor.a );`,OI=`vec3 packNormalToRGB( const in vec3 normal ) {
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
}`,kI=`#ifdef PREMULTIPLIED_ALPHA
	gl_FragColor.rgb *= gl_FragColor.a;
#endif`,UI=`vec4 mvPosition = vec4( transformed, 1.0 );
#ifdef USE_BATCHING
	mvPosition = batchingMatrix * mvPosition;
#endif
#ifdef USE_INSTANCING
	mvPosition = instanceMatrix * mvPosition;
#endif
mvPosition = modelViewMatrix * mvPosition;
gl_Position = projectionMatrix * mvPosition;`,BI=`#ifdef DITHERING
	gl_FragColor.rgb = dithering( gl_FragColor.rgb );
#endif`,VI=`#ifdef DITHERING
	vec3 dithering( vec3 color ) {
		float grid_position = rand( gl_FragCoord.xy );
		vec3 dither_shift_RGB = vec3( 0.25 / 255.0, -0.25 / 255.0, 0.25 / 255.0 );
		dither_shift_RGB = mix( 2.0 * dither_shift_RGB, -2.0 * dither_shift_RGB, grid_position );
		return color + dither_shift_RGB;
	}
#endif`,HI=`float roughnessFactor = roughness;
#ifdef USE_ROUGHNESSMAP
	vec4 texelRoughness = texture2D( roughnessMap, vRoughnessMapUv );
	roughnessFactor *= texelRoughness.g;
#endif`,zI=`#ifdef USE_ROUGHNESSMAP
	uniform sampler2D roughnessMap;
#endif`,GI=`#if NUM_SPOT_LIGHT_COORDS > 0
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
#endif`,WI=`#if NUM_SPOT_LIGHT_COORDS > 0
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
#endif`,jI=`#if ( defined( USE_SHADOWMAP ) && ( NUM_DIR_LIGHT_SHADOWS > 0 || NUM_POINT_LIGHT_SHADOWS > 0 ) ) || ( NUM_SPOT_LIGHT_COORDS > 0 )
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
#endif`,$I=`float getShadowMask() {
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
}`,qI=`#ifdef USE_SKINNING
	mat4 boneMatX = getBoneMatrix( skinIndex.x );
	mat4 boneMatY = getBoneMatrix( skinIndex.y );
	mat4 boneMatZ = getBoneMatrix( skinIndex.z );
	mat4 boneMatW = getBoneMatrix( skinIndex.w );
#endif`,XI=`#ifdef USE_SKINNING
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
#endif`,YI=`#ifdef USE_SKINNING
	vec4 skinVertex = bindMatrix * vec4( transformed, 1.0 );
	vec4 skinned = vec4( 0.0 );
	skinned += boneMatX * skinVertex * skinWeight.x;
	skinned += boneMatY * skinVertex * skinWeight.y;
	skinned += boneMatZ * skinVertex * skinWeight.z;
	skinned += boneMatW * skinVertex * skinWeight.w;
	transformed = ( bindMatrixInverse * skinned ).xyz;
#endif`,ZI=`#ifdef USE_SKINNING
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
#endif`,JI=`float specularStrength;
#ifdef USE_SPECULARMAP
	vec4 texelSpecular = texture2D( specularMap, vSpecularMapUv );
	specularStrength = texelSpecular.r;
#else
	specularStrength = 1.0;
#endif`,KI=`#ifdef USE_SPECULARMAP
	uniform sampler2D specularMap;
#endif`,QI=`#if defined( TONE_MAPPING )
	gl_FragColor.rgb = toneMapping( gl_FragColor.rgb );
#endif`,eA=`#ifndef saturate
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
vec3 CustomToneMapping( vec3 color ) { return color; }`,tA=`#ifdef USE_TRANSMISSION
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
#endif`,nA=`#ifdef USE_TRANSMISSION
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
#endif`,iA=`#if defined( USE_UV ) || defined( USE_ANISOTROPY )
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
#endif`,rA=`#if defined( USE_UV ) || defined( USE_ANISOTROPY )
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
#endif`,sA=`#if defined( USE_UV ) || defined( USE_ANISOTROPY )
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
#endif`,oA=`#if defined( USE_ENVMAP ) || defined( DISTANCE ) || defined ( USE_SHADOWMAP ) || defined ( USE_TRANSMISSION ) || NUM_SPOT_LIGHT_COORDS > 0
	vec4 worldPosition = vec4( transformed, 1.0 );
	#ifdef USE_BATCHING
		worldPosition = batchingMatrix * worldPosition;
	#endif
	#ifdef USE_INSTANCING
		worldPosition = instanceMatrix * worldPosition;
	#endif
	worldPosition = modelMatrix * worldPosition;
#endif`,aA=`varying vec2 vUv;
uniform mat3 uvTransform;
void main() {
	vUv = ( uvTransform * vec3( uv, 1 ) ).xy;
	gl_Position = vec4( position.xy, 1.0, 1.0 );
}`,cA=`uniform sampler2D t2D;
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
}`,lA=`varying vec3 vWorldDirection;
#include <common>
void main() {
	vWorldDirection = transformDirection( position, modelMatrix );
	#include <begin_vertex>
	#include <project_vertex>
	gl_Position.z = gl_Position.w;
}`,uA=`#ifdef ENVMAP_TYPE_CUBE
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
}`,dA=`varying vec3 vWorldDirection;
#include <common>
void main() {
	vWorldDirection = transformDirection( position, modelMatrix );
	#include <begin_vertex>
	#include <project_vertex>
	gl_Position.z = gl_Position.w;
}`,fA=`uniform samplerCube tCube;
uniform float tFlip;
uniform float opacity;
varying vec3 vWorldDirection;
void main() {
	vec4 texColor = textureCube( tCube, vec3( tFlip * vWorldDirection.x, vWorldDirection.yz ) );
	gl_FragColor = texColor;
	gl_FragColor.a *= opacity;
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
}`,hA=`#include <common>
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
}`,pA=`#if DEPTH_PACKING == 3200
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
}`,mA=`#define DISTANCE
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
}`,gA=`#define DISTANCE
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
}`,vA=`varying vec3 vWorldDirection;
#include <common>
void main() {
	vWorldDirection = transformDirection( position, modelMatrix );
	#include <begin_vertex>
	#include <project_vertex>
}`,yA=`uniform sampler2D tEquirect;
varying vec3 vWorldDirection;
#include <common>
void main() {
	vec3 direction = normalize( vWorldDirection );
	vec2 sampleUV = equirectUv( direction );
	gl_FragColor = texture2D( tEquirect, sampleUV );
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
}`,_A=`uniform float scale;
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
}`,xA=`uniform vec3 diffuse;
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
}`,MA=`#include <common>
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
}`,bA=`uniform vec3 diffuse;
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
}`,EA=`#define LAMBERT
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
}`,SA=`#define LAMBERT
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
}`,TA=`#define MATCAP
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
}`,CA=`#define MATCAP
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
}`,wA=`#define NORMAL
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
}`,DA=`#define NORMAL
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
}`,IA=`#define PHONG
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
}`,AA=`#define PHONG
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
}`,RA=`#define STANDARD
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
}`,NA=`#define STANDARD
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
}`,PA=`#define TOON
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
}`,LA=`#define TOON
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
}`,FA=`uniform float size;
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
}`,OA=`uniform vec3 diffuse;
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
}`,kA=`#include <common>
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
}`,UA=`uniform vec3 color;
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
}`,BA=`uniform float rotation;
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
}`,VA=`uniform vec3 diffuse;
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
}`,Ge={alphahash_fragment:cD,alphahash_pars_fragment:lD,alphamap_fragment:uD,alphamap_pars_fragment:dD,alphatest_fragment:fD,alphatest_pars_fragment:hD,aomap_fragment:pD,aomap_pars_fragment:mD,batching_pars_vertex:gD,batching_vertex:vD,begin_vertex:yD,beginnormal_vertex:_D,bsdfs:xD,iridescence_fragment:MD,bumpmap_pars_fragment:bD,clipping_planes_fragment:ED,clipping_planes_pars_fragment:SD,clipping_planes_pars_vertex:TD,clipping_planes_vertex:CD,color_fragment:wD,color_pars_fragment:DD,color_pars_vertex:ID,color_vertex:AD,common:RD,cube_uv_reflection_fragment:ND,defaultnormal_vertex:PD,displacementmap_pars_vertex:LD,displacementmap_vertex:FD,emissivemap_fragment:OD,emissivemap_pars_fragment:kD,colorspace_fragment:UD,colorspace_pars_fragment:BD,envmap_fragment:VD,envmap_common_pars_fragment:HD,envmap_pars_fragment:zD,envmap_pars_vertex:GD,envmap_physical_pars_fragment:eI,envmap_vertex:WD,fog_vertex:jD,fog_pars_vertex:$D,fog_fragment:qD,fog_pars_fragment:XD,gradientmap_pars_fragment:YD,lightmap_pars_fragment:ZD,lights_lambert_fragment:JD,lights_lambert_pars_fragment:KD,lights_pars_begin:QD,lights_toon_fragment:tI,lights_toon_pars_fragment:nI,lights_phong_fragment:iI,lights_phong_pars_fragment:rI,lights_physical_fragment:sI,lights_physical_pars_fragment:oI,lights_fragment_begin:aI,lights_fragment_maps:cI,lights_fragment_end:lI,logdepthbuf_fragment:uI,logdepthbuf_pars_fragment:dI,logdepthbuf_pars_vertex:fI,logdepthbuf_vertex:hI,map_fragment:pI,map_pars_fragment:mI,map_particle_fragment:gI,map_particle_pars_fragment:vI,metalnessmap_fragment:yI,metalnessmap_pars_fragment:_I,morphinstance_vertex:xI,morphcolor_vertex:MI,morphnormal_vertex:bI,morphtarget_pars_vertex:EI,morphtarget_vertex:SI,normal_fragment_begin:TI,normal_fragment_maps:CI,normal_pars_fragment:wI,normal_pars_vertex:DI,normal_vertex:II,normalmap_pars_fragment:AI,clearcoat_normal_fragment_begin:RI,clearcoat_normal_fragment_maps:NI,clearcoat_pars_fragment:PI,iridescence_pars_fragment:LI,opaque_fragment:FI,packing:OI,premultiplied_alpha_fragment:kI,project_vertex:UI,dithering_fragment:BI,dithering_pars_fragment:VI,roughnessmap_fragment:HI,roughnessmap_pars_fragment:zI,shadowmap_pars_fragment:GI,shadowmap_pars_vertex:WI,shadowmap_vertex:jI,shadowmask_pars_fragment:$I,skinbase_vertex:qI,skinning_pars_vertex:XI,skinning_vertex:YI,skinnormal_vertex:ZI,specularmap_fragment:JI,specularmap_pars_fragment:KI,tonemapping_fragment:QI,tonemapping_pars_fragment:eA,transmission_fragment:tA,transmission_pars_fragment:nA,uv_pars_fragment:iA,uv_pars_vertex:rA,uv_vertex:sA,worldpos_vertex:oA,background_vert:aA,background_frag:cA,backgroundCube_vert:lA,backgroundCube_frag:uA,cube_vert:dA,cube_frag:fA,depth_vert:hA,depth_frag:pA,distance_vert:mA,distance_frag:gA,equirect_vert:vA,equirect_frag:yA,linedashed_vert:_A,linedashed_frag:xA,meshbasic_vert:MA,meshbasic_frag:bA,meshlambert_vert:EA,meshlambert_frag:SA,meshmatcap_vert:TA,meshmatcap_frag:CA,meshnormal_vert:wA,meshnormal_frag:DA,meshphong_vert:IA,meshphong_frag:AA,meshphysical_vert:RA,meshphysical_frag:NA,meshtoon_vert:PA,meshtoon_frag:LA,points_vert:FA,points_frag:OA,shadow_vert:kA,shadow_frag:UA,sprite_vert:BA,sprite_frag:VA},ae={common:{diffuse:{value:new Xe(16777215)},opacity:{value:1},map:{value:null},mapTransform:{value:new Be},alphaMap:{value:null},alphaMapTransform:{value:new Be},alphaTest:{value:0}},specularmap:{specularMap:{value:null},specularMapTransform:{value:new Be}},envmap:{envMap:{value:null},envMapRotation:{value:new Be},flipEnvMap:{value:-1},reflectivity:{value:1},ior:{value:1.5},refractionRatio:{value:.98},dfgLUT:{value:null}},aomap:{aoMap:{value:null},aoMapIntensity:{value:1},aoMapTransform:{value:new Be}},lightmap:{lightMap:{value:null},lightMapIntensity:{value:1},lightMapTransform:{value:new Be}},bumpmap:{bumpMap:{value:null},bumpMapTransform:{value:new Be},bumpScale:{value:1}},normalmap:{normalMap:{value:null},normalMapTransform:{value:new Be},normalScale:{value:new pt(1,1)}},displacementmap:{displacementMap:{value:null},displacementMapTransform:{value:new Be},displacementScale:{value:1},displacementBias:{value:0}},emissivemap:{emissiveMap:{value:null},emissiveMapTransform:{value:new Be}},metalnessmap:{metalnessMap:{value:null},metalnessMapTransform:{value:new Be}},roughnessmap:{roughnessMap:{value:null},roughnessMapTransform:{value:new Be}},gradientmap:{gradientMap:{value:null}},fog:{fogDensity:{value:25e-5},fogNear:{value:1},fogFar:{value:2e3},fogColor:{value:new Xe(16777215)}},lights:{ambientLightColor:{value:[]},lightProbe:{value:[]},directionalLights:{value:[],properties:{direction:{},color:{}}},directionalLightShadows:{value:[],properties:{shadowIntensity:1,shadowBias:{},shadowNormalBias:{},shadowRadius:{},shadowMapSize:{}}},directionalShadowMatrix:{value:[]},spotLights:{value:[],properties:{color:{},position:{},direction:{},distance:{},coneCos:{},penumbraCos:{},decay:{}}},spotLightShadows:{value:[],properties:{shadowIntensity:1,shadowBias:{},shadowNormalBias:{},shadowRadius:{},shadowMapSize:{}}},spotLightMap:{value:[]},spotLightMatrix:{value:[]},pointLights:{value:[],properties:{color:{},position:{},decay:{},distance:{}}},pointLightShadows:{value:[],properties:{shadowIntensity:1,shadowBias:{},shadowNormalBias:{},shadowRadius:{},shadowMapSize:{},shadowCameraNear:{},shadowCameraFar:{}}},pointShadowMatrix:{value:[]},hemisphereLights:{value:[],properties:{direction:{},skyColor:{},groundColor:{}}},rectAreaLights:{value:[],properties:{color:{},position:{},width:{},height:{}}},ltc_1:{value:null},ltc_2:{value:null}},points:{diffuse:{value:new Xe(16777215)},opacity:{value:1},size:{value:1},scale:{value:1},map:{value:null},alphaMap:{value:null},alphaMapTransform:{value:new Be},alphaTest:{value:0},uvTransform:{value:new Be}},sprite:{diffuse:{value:new Xe(16777215)},opacity:{value:1},center:{value:new pt(.5,.5)},rotation:{value:0},map:{value:null},mapTransform:{value:new Be},alphaMap:{value:null},alphaMapTransform:{value:new Be},alphaTest:{value:0}}},Li={basic:{uniforms:mn([ae.common,ae.specularmap,ae.envmap,ae.aomap,ae.lightmap,ae.fog]),vertexShader:Ge.meshbasic_vert,fragmentShader:Ge.meshbasic_frag},lambert:{uniforms:mn([ae.common,ae.specularmap,ae.envmap,ae.aomap,ae.lightmap,ae.emissivemap,ae.bumpmap,ae.normalmap,ae.displacementmap,ae.fog,ae.lights,{emissive:{value:new Xe(0)},envMapIntensity:{value:1}}]),vertexShader:Ge.meshlambert_vert,fragmentShader:Ge.meshlambert_frag},phong:{uniforms:mn([ae.common,ae.specularmap,ae.envmap,ae.aomap,ae.lightmap,ae.emissivemap,ae.bumpmap,ae.normalmap,ae.displacementmap,ae.fog,ae.lights,{emissive:{value:new Xe(0)},specular:{value:new Xe(1118481)},shininess:{value:30},envMapIntensity:{value:1}}]),vertexShader:Ge.meshphong_vert,fragmentShader:Ge.meshphong_frag},standard:{uniforms:mn([ae.common,ae.envmap,ae.aomap,ae.lightmap,ae.emissivemap,ae.bumpmap,ae.normalmap,ae.displacementmap,ae.roughnessmap,ae.metalnessmap,ae.fog,ae.lights,{emissive:{value:new Xe(0)},roughness:{value:1},metalness:{value:0},envMapIntensity:{value:1}}]),vertexShader:Ge.meshphysical_vert,fragmentShader:Ge.meshphysical_frag},toon:{uniforms:mn([ae.common,ae.aomap,ae.lightmap,ae.emissivemap,ae.bumpmap,ae.normalmap,ae.displacementmap,ae.gradientmap,ae.fog,ae.lights,{emissive:{value:new Xe(0)}}]),vertexShader:Ge.meshtoon_vert,fragmentShader:Ge.meshtoon_frag},matcap:{uniforms:mn([ae.common,ae.bumpmap,ae.normalmap,ae.displacementmap,ae.fog,{matcap:{value:null}}]),vertexShader:Ge.meshmatcap_vert,fragmentShader:Ge.meshmatcap_frag},points:{uniforms:mn([ae.points,ae.fog]),vertexShader:Ge.points_vert,fragmentShader:Ge.points_frag},dashed:{uniforms:mn([ae.common,ae.fog,{scale:{value:1},dashSize:{value:1},totalSize:{value:2}}]),vertexShader:Ge.linedashed_vert,fragmentShader:Ge.linedashed_frag},depth:{uniforms:mn([ae.common,ae.displacementmap]),vertexShader:Ge.depth_vert,fragmentShader:Ge.depth_frag},normal:{uniforms:mn([ae.common,ae.bumpmap,ae.normalmap,ae.displacementmap,{opacity:{value:1}}]),vertexShader:Ge.meshnormal_vert,fragmentShader:Ge.meshnormal_frag},sprite:{uniforms:mn([ae.sprite,ae.fog]),vertexShader:Ge.sprite_vert,fragmentShader:Ge.sprite_frag},background:{uniforms:{uvTransform:{value:new Be},t2D:{value:null},backgroundIntensity:{value:1}},vertexShader:Ge.background_vert,fragmentShader:Ge.background_frag},backgroundCube:{uniforms:{envMap:{value:null},flipEnvMap:{value:-1},backgroundBlurriness:{value:0},backgroundIntensity:{value:1},backgroundRotation:{value:new Be}},vertexShader:Ge.backgroundCube_vert,fragmentShader:Ge.backgroundCube_frag},cube:{uniforms:{tCube:{value:null},tFlip:{value:-1},opacity:{value:1}},vertexShader:Ge.cube_vert,fragmentShader:Ge.cube_frag},equirect:{uniforms:{tEquirect:{value:null}},vertexShader:Ge.equirect_vert,fragmentShader:Ge.equirect_frag},distance:{uniforms:mn([ae.common,ae.displacementmap,{referencePosition:{value:new k},nearDistance:{value:1},farDistance:{value:1e3}}]),vertexShader:Ge.distance_vert,fragmentShader:Ge.distance_frag},shadow:{uniforms:mn([ae.lights,ae.fog,{color:{value:new Xe(0)},opacity:{value:1}}]),vertexShader:Ge.shadow_vert,fragmentShader:Ge.shadow_frag}};Li.physical={uniforms:mn([Li.standard.uniforms,{clearcoat:{value:0},clearcoatMap:{value:null},clearcoatMapTransform:{value:new Be},clearcoatNormalMap:{value:null},clearcoatNormalMapTransform:{value:new Be},clearcoatNormalScale:{value:new pt(1,1)},clearcoatRoughness:{value:0},clearcoatRoughnessMap:{value:null},clearcoatRoughnessMapTransform:{value:new Be},dispersion:{value:0},iridescence:{value:0},iridescenceMap:{value:null},iridescenceMapTransform:{value:new Be},iridescenceIOR:{value:1.3},iridescenceThicknessMinimum:{value:100},iridescenceThicknessMaximum:{value:400},iridescenceThicknessMap:{value:null},iridescenceThicknessMapTransform:{value:new Be},sheen:{value:0},sheenColor:{value:new Xe(0)},sheenColorMap:{value:null},sheenColorMapTransform:{value:new Be},sheenRoughness:{value:1},sheenRoughnessMap:{value:null},sheenRoughnessMapTransform:{value:new Be},transmission:{value:0},transmissionMap:{value:null},transmissionMapTransform:{value:new Be},transmissionSamplerSize:{value:new pt},transmissionSamplerMap:{value:null},thickness:{value:0},thicknessMap:{value:null},thicknessMapTransform:{value:new Be},attenuationDistance:{value:0},attenuationColor:{value:new Xe(0)},specularColor:{value:new Xe(1,1,1)},specularColorMap:{value:null},specularColorMapTransform:{value:new Be},specularIntensity:{value:1},specularIntensityMap:{value:null},specularIntensityMapTransform:{value:new Be},anisotropyVector:{value:new pt},anisotropyMap:{value:null},anisotropyMapTransform:{value:new Be}}]),vertexShader:Ge.meshphysical_vert,fragmentShader:Ge.meshphysical_frag};var Xd={r:0,b:0,g:0},Is=new Ss,HA=new Bt;function zA(n,e,t,i,r,s){let o=new Xe(0),a=r===!0?0:1,c,l,u=null,f=0,d=null;function h(M){let T=M.isScene===!0?M.background:null;if(T&&T.isTexture){let S=M.backgroundBlurriness>0;T=e.get(T,S)}return T}function g(M){let T=!1,S=h(M);S===null?m(o,a):S&&S.isColor&&(m(S,1),T=!0);let I=n.xr.getEnvironmentBlendMode();I==="additive"?t.buffers.color.setClear(0,0,0,1,s):I==="alpha-blend"&&t.buffers.color.setClear(0,0,0,0,s),(n.autoClear||T)&&(t.buffers.depth.setTest(!0),t.buffers.depth.setMask(!0),t.buffers.color.setMask(!0),n.clear(n.autoClearColor,n.autoClearDepth,n.autoClearStencil))}function x(M,T){let S=h(T);S&&(S.isCubeTexture||S.mapping===sc)?(l===void 0&&(l=new Zn(new Ao(1,1,1),new On({name:"BackgroundCubeMaterial",uniforms:Ds(Li.backgroundCube.uniforms),vertexShader:Li.backgroundCube.vertexShader,fragmentShader:Li.backgroundCube.fragmentShader,side:xn,depthTest:!1,depthWrite:!1,fog:!1,allowOverride:!1})),l.geometry.deleteAttribute("normal"),l.geometry.deleteAttribute("uv"),l.onBeforeRender=function(I,D,A){this.matrixWorld.copyPosition(A.matrixWorld)},Object.defineProperty(l.material,"envMap",{get:function(){return this.uniforms.envMap.value}}),i.update(l)),Is.copy(T.backgroundRotation),Is.x*=-1,Is.y*=-1,Is.z*=-1,S.isCubeTexture&&S.isRenderTargetTexture===!1&&(Is.y*=-1,Is.z*=-1),l.material.uniforms.envMap.value=S,l.material.uniforms.flipEnvMap.value=S.isCubeTexture&&S.isRenderTargetTexture===!1?-1:1,l.material.uniforms.backgroundBlurriness.value=T.backgroundBlurriness,l.material.uniforms.backgroundIntensity.value=T.backgroundIntensity,l.material.uniforms.backgroundRotation.value.setFromMatrix4(HA.makeRotationFromEuler(Is)),l.material.toneMapped=rt.getTransfer(S.colorSpace)!==dt,(u!==S||f!==S.version||d!==n.toneMapping)&&(l.material.needsUpdate=!0,u=S,f=S.version,d=n.toneMapping),l.layers.enableAll(),M.unshift(l,l.geometry,l.material,0,0,null)):S&&S.isTexture&&(c===void 0&&(c=new Zn(new Qa(2,2),new On({name:"BackgroundMaterial",uniforms:Ds(Li.background.uniforms),vertexShader:Li.background.vertexShader,fragmentShader:Li.background.fragmentShader,side:ir,depthTest:!1,depthWrite:!1,fog:!1,allowOverride:!1})),c.geometry.deleteAttribute("normal"),Object.defineProperty(c.material,"map",{get:function(){return this.uniforms.t2D.value}}),i.update(c)),c.material.uniforms.t2D.value=S,c.material.uniforms.backgroundIntensity.value=T.backgroundIntensity,c.material.toneMapped=rt.getTransfer(S.colorSpace)!==dt,S.matrixAutoUpdate===!0&&S.updateMatrix(),c.material.uniforms.uvTransform.value.copy(S.matrix),(u!==S||f!==S.version||d!==n.toneMapping)&&(c.material.needsUpdate=!0,u=S,f=S.version,d=n.toneMapping),c.layers.enableAll(),M.unshift(c,c.geometry,c.material,0,0,null))}function m(M,T){M.getRGB(Xd,gg(n)),t.buffers.color.setClear(Xd.r,Xd.g,Xd.b,T,s)}function p(){l!==void 0&&(l.geometry.dispose(),l.material.dispose(),l=void 0),c!==void 0&&(c.geometry.dispose(),c.material.dispose(),c=void 0)}return{getClearColor:function(){return o},setClearColor:function(M,T=1){o.set(M),a=T,m(o,a)},getClearAlpha:function(){return a},setClearAlpha:function(M){a=M,m(o,a)},render:g,addToRenderList:x,dispose:p}}function GA(n,e){let t=n.getParameter(n.MAX_VERTEX_ATTRIBS),i={},r=d(null),s=r,o=!1;function a(C,U,V,W,B){let H=!1,L=f(C,W,V,U);s!==L&&(s=L,l(s.object)),H=h(C,W,V,B),H&&g(C,W,V,B),B!==null&&e.update(B,n.ELEMENT_ARRAY_BUFFER),(H||o)&&(o=!1,S(C,U,V,W),B!==null&&n.bindBuffer(n.ELEMENT_ARRAY_BUFFER,e.get(B).buffer))}function c(){return n.createVertexArray()}function l(C){return n.bindVertexArray(C)}function u(C){return n.deleteVertexArray(C)}function f(C,U,V,W){let B=W.wireframe===!0,H=i[U.id];H===void 0&&(H={},i[U.id]=H);let L=C.isInstancedMesh===!0?C.id:0,Q=H[L];Q===void 0&&(Q={},H[L]=Q);let Z=Q[V.id];Z===void 0&&(Z={},Q[V.id]=Z);let ue=Z[B];return ue===void 0&&(ue=d(c()),Z[B]=ue),ue}function d(C){let U=[],V=[],W=[];for(let B=0;B<t;B++)U[B]=0,V[B]=0,W[B]=0;return{geometry:null,program:null,wireframe:!1,newAttributes:U,enabledAttributes:V,attributeDivisors:W,object:C,attributes:{},index:null}}function h(C,U,V,W){let B=s.attributes,H=U.attributes,L=0,Q=V.getAttributes();for(let Z in Q)if(Q[Z].location>=0){let ge=B[Z],fe=H[Z];if(fe===void 0&&(Z==="instanceMatrix"&&C.instanceMatrix&&(fe=C.instanceMatrix),Z==="instanceColor"&&C.instanceColor&&(fe=C.instanceColor)),ge===void 0||ge.attribute!==fe||fe&&ge.data!==fe.data)return!0;L++}return s.attributesNum!==L||s.index!==W}function g(C,U,V,W){let B={},H=U.attributes,L=0,Q=V.getAttributes();for(let Z in Q)if(Q[Z].location>=0){let ge=H[Z];ge===void 0&&(Z==="instanceMatrix"&&C.instanceMatrix&&(ge=C.instanceMatrix),Z==="instanceColor"&&C.instanceColor&&(ge=C.instanceColor));let fe={};fe.attribute=ge,ge&&ge.data&&(fe.data=ge.data),B[Z]=fe,L++}s.attributes=B,s.attributesNum=L,s.index=W}function x(){let C=s.newAttributes;for(let U=0,V=C.length;U<V;U++)C[U]=0}function m(C){p(C,0)}function p(C,U){let V=s.newAttributes,W=s.enabledAttributes,B=s.attributeDivisors;V[C]=1,W[C]===0&&(n.enableVertexAttribArray(C),W[C]=1),B[C]!==U&&(n.vertexAttribDivisor(C,U),B[C]=U)}function M(){let C=s.newAttributes,U=s.enabledAttributes;for(let V=0,W=U.length;V<W;V++)U[V]!==C[V]&&(n.disableVertexAttribArray(V),U[V]=0)}function T(C,U,V,W,B,H,L){L===!0?n.vertexAttribIPointer(C,U,V,B,H):n.vertexAttribPointer(C,U,V,W,B,H)}function S(C,U,V,W){x();let B=W.attributes,H=V.getAttributes(),L=U.defaultAttributeValues;for(let Q in H){let Z=H[Q];if(Z.location>=0){let ue=B[Q];if(ue===void 0&&(Q==="instanceMatrix"&&C.instanceMatrix&&(ue=C.instanceMatrix),Q==="instanceColor"&&C.instanceColor&&(ue=C.instanceColor)),ue!==void 0){let ge=ue.normalized,fe=ue.itemSize,ze=e.get(ue);if(ze===void 0)continue;let Tt=ze.buffer,St=ze.type,X=ze.bytesPerElement,ne=St===n.INT||St===n.UNSIGNED_INT||ue.gpuType===cd;if(ue.isInterleavedBufferAttribute){let se=ue.data,Ve=se.stride,Ie=ue.offset;if(se.isInstancedInterleavedBuffer){for(let Fe=0;Fe<Z.locationSize;Fe++)p(Z.location+Fe,se.meshPerAttribute);C.isInstancedMesh!==!0&&W._maxInstanceCount===void 0&&(W._maxInstanceCount=se.meshPerAttribute*se.count)}else for(let Fe=0;Fe<Z.locationSize;Fe++)m(Z.location+Fe);n.bindBuffer(n.ARRAY_BUFFER,Tt);for(let Fe=0;Fe<Z.locationSize;Fe++)T(Z.location+Fe,fe/Z.locationSize,St,ge,Ve*X,(Ie+fe/Z.locationSize*Fe)*X,ne)}else{if(ue.isInstancedBufferAttribute){for(let se=0;se<Z.locationSize;se++)p(Z.location+se,ue.meshPerAttribute);C.isInstancedMesh!==!0&&W._maxInstanceCount===void 0&&(W._maxInstanceCount=ue.meshPerAttribute*ue.count)}else for(let se=0;se<Z.locationSize;se++)m(Z.location+se);n.bindBuffer(n.ARRAY_BUFFER,Tt);for(let se=0;se<Z.locationSize;se++)T(Z.location+se,fe/Z.locationSize,St,ge,fe*X,fe/Z.locationSize*se*X,ne)}}else if(L!==void 0){let ge=L[Q];if(ge!==void 0)switch(ge.length){case 2:n.vertexAttrib2fv(Z.location,ge);break;case 3:n.vertexAttrib3fv(Z.location,ge);break;case 4:n.vertexAttrib4fv(Z.location,ge);break;default:n.vertexAttrib1fv(Z.location,ge)}}}}M()}function I(){E();for(let C in i){let U=i[C];for(let V in U){let W=U[V];for(let B in W){let H=W[B];for(let L in H)u(H[L].object),delete H[L];delete W[B]}}delete i[C]}}function D(C){if(i[C.id]===void 0)return;let U=i[C.id];for(let V in U){let W=U[V];for(let B in W){let H=W[B];for(let L in H)u(H[L].object),delete H[L];delete W[B]}}delete i[C.id]}function A(C){for(let U in i){let V=i[U];for(let W in V){let B=V[W];if(B[C.id]===void 0)continue;let H=B[C.id];for(let L in H)u(H[L].object),delete H[L];delete B[C.id]}}}function y(C){for(let U in i){let V=i[U],W=C.isInstancedMesh===!0?C.id:0,B=V[W];if(B!==void 0){for(let H in B){let L=B[H];for(let Q in L)u(L[Q].object),delete L[Q];delete B[H]}delete V[W],Object.keys(V).length===0&&delete i[U]}}}function E(){q(),o=!0,s!==r&&(s=r,l(s.object))}function q(){r.geometry=null,r.program=null,r.wireframe=!1}return{setup:a,reset:E,resetDefaultState:q,dispose:I,releaseStatesOfGeometry:D,releaseStatesOfObject:y,releaseStatesOfProgram:A,initAttributes:x,enableAttribute:m,disableUnusedAttributes:M}}function WA(n,e,t){let i;function r(l){i=l}function s(l,u){n.drawArrays(i,l,u),t.update(u,i,1)}function o(l,u,f){f!==0&&(n.drawArraysInstanced(i,l,u,f),t.update(u,i,f))}function a(l,u,f){if(f===0)return;e.get("WEBGL_multi_draw").multiDrawArraysWEBGL(i,l,0,u,0,f);let h=0;for(let g=0;g<f;g++)h+=u[g];t.update(h,i,1)}function c(l,u,f,d){if(f===0)return;let h=e.get("WEBGL_multi_draw");if(h===null)for(let g=0;g<l.length;g++)o(l[g],u[g],d[g]);else{h.multiDrawArraysInstancedWEBGL(i,l,0,u,0,d,0,f);let g=0;for(let x=0;x<f;x++)g+=u[x]*d[x];t.update(g,i,1)}}this.setMode=r,this.render=s,this.renderInstances=o,this.renderMultiDraw=a,this.renderMultiDrawInstances=c}function jA(n,e,t,i){let r;function s(){if(r!==void 0)return r;if(e.has("EXT_texture_filter_anisotropic")===!0){let A=e.get("EXT_texture_filter_anisotropic");r=n.getParameter(A.MAX_TEXTURE_MAX_ANISOTROPY_EXT)}else r=0;return r}function o(A){return!(A!==Jn&&i.convert(A)!==n.getParameter(n.IMPLEMENTATION_COLOR_READ_FORMAT))}function a(A){let y=A===Ni&&(e.has("EXT_color_buffer_half_float")||e.has("EXT_color_buffer_float"));return!(A!==Un&&i.convert(A)!==n.getParameter(n.IMPLEMENTATION_COLOR_READ_TYPE)&&A!==gi&&!y)}function c(A){if(A==="highp"){if(n.getShaderPrecisionFormat(n.VERTEX_SHADER,n.HIGH_FLOAT).precision>0&&n.getShaderPrecisionFormat(n.FRAGMENT_SHADER,n.HIGH_FLOAT).precision>0)return"highp";A="mediump"}return A==="mediump"&&n.getShaderPrecisionFormat(n.VERTEX_SHADER,n.MEDIUM_FLOAT).precision>0&&n.getShaderPrecisionFormat(n.FRAGMENT_SHADER,n.MEDIUM_FLOAT).precision>0?"mediump":"lowp"}let l=t.precision!==void 0?t.precision:"highp",u=c(l);u!==l&&(Le("WebGLRenderer:",l,"not supported, using",u,"instead."),l=u);let f=t.logarithmicDepthBuffer===!0,d=t.reversedDepthBuffer===!0&&e.has("EXT_clip_control"),h=n.getParameter(n.MAX_TEXTURE_IMAGE_UNITS),g=n.getParameter(n.MAX_VERTEX_TEXTURE_IMAGE_UNITS),x=n.getParameter(n.MAX_TEXTURE_SIZE),m=n.getParameter(n.MAX_CUBE_MAP_TEXTURE_SIZE),p=n.getParameter(n.MAX_VERTEX_ATTRIBS),M=n.getParameter(n.MAX_VERTEX_UNIFORM_VECTORS),T=n.getParameter(n.MAX_VARYING_VECTORS),S=n.getParameter(n.MAX_FRAGMENT_UNIFORM_VECTORS),I=n.getParameter(n.MAX_SAMPLES),D=n.getParameter(n.SAMPLES);return{isWebGL2:!0,getMaxAnisotropy:s,getMaxPrecision:c,textureFormatReadable:o,textureTypeReadable:a,precision:l,logarithmicDepthBuffer:f,reversedDepthBuffer:d,maxTextures:h,maxVertexTextures:g,maxTextureSize:x,maxCubemapSize:m,maxAttributes:p,maxVertexUniforms:M,maxVaryings:T,maxFragmentUniforms:S,maxSamples:I,samples:D}}function $A(n){let e=this,t=null,i=0,r=!1,s=!1,o=new Ti,a=new Be,c={value:null,needsUpdate:!1};this.uniform=c,this.numPlanes=0,this.numIntersection=0,this.init=function(f,d){let h=f.length!==0||d||i!==0||r;return r=d,i=f.length,h},this.beginShadows=function(){s=!0,u(null)},this.endShadows=function(){s=!1},this.setGlobalState=function(f,d){t=u(f,d,0)},this.setState=function(f,d,h){let g=f.clippingPlanes,x=f.clipIntersection,m=f.clipShadows,p=n.get(f);if(!r||g===null||g.length===0||s&&!m)s?u(null):l();else{let M=s?0:i,T=M*4,S=p.clippingState||null;c.value=S,S=u(g,d,T,h);for(let I=0;I!==T;++I)S[I]=t[I];p.clippingState=S,this.numIntersection=x?this.numPlanes:0,this.numPlanes+=M}};function l(){c.value!==t&&(c.value=t,c.needsUpdate=i>0),e.numPlanes=i,e.numIntersection=0}function u(f,d,h,g){let x=f!==null?f.length:0,m=null;if(x!==0){if(m=c.value,g!==!0||m===null){let p=h+x*4,M=d.matrixWorldInverse;a.getNormalMatrix(M),(m===null||m.length<p)&&(m=new Float32Array(p));for(let T=0,S=h;T!==x;++T,S+=4)o.copy(f[T]).applyMatrix4(M,a),o.normal.toArray(m,S),m[S+3]=o.constant}c.value=m,c.needsUpdate=!0}return e.numPlanes=x,e.numIntersection=0,m}}var zr=4,tM=[.125,.215,.35,.446,.526,.582],Rs=20,qA=256,fc=new nc,nM=new Xe,xg=null,Mg=0,bg=0,Eg=!1,XA=new k,Zd=class{constructor(e){this._renderer=e,this._pingPongRenderTarget=null,this._lodMax=0,this._cubeSize=0,this._sizeLods=[],this._sigmas=[],this._lodMeshes=[],this._backgroundBox=null,this._cubemapMaterial=null,this._equirectMaterial=null,this._blurMaterial=null,this._ggxMaterial=null}fromScene(e,t=0,i=.1,r=100,s={}){let{size:o=256,position:a=XA}=s;xg=this._renderer.getRenderTarget(),Mg=this._renderer.getActiveCubeFace(),bg=this._renderer.getActiveMipmapLevel(),Eg=this._renderer.xr.enabled,this._renderer.xr.enabled=!1,this._setSize(o);let c=this._allocateTargets();return c.depthBuffer=!0,this._sceneToCubeUV(e,i,r,c,a),t>0&&this._blur(c,0,0,t),this._applyPMREM(c),this._cleanup(c),c}fromEquirectangular(e,t=null){return this._fromTexture(e,t)}fromCubemap(e,t=null){return this._fromTexture(e,t)}compileCubemapShader(){this._cubemapMaterial===null&&(this._cubemapMaterial=sM(),this._compileMaterial(this._cubemapMaterial))}compileEquirectangularShader(){this._equirectMaterial===null&&(this._equirectMaterial=rM(),this._compileMaterial(this._equirectMaterial))}dispose(){this._dispose(),this._cubemapMaterial!==null&&this._cubemapMaterial.dispose(),this._equirectMaterial!==null&&this._equirectMaterial.dispose(),this._backgroundBox!==null&&(this._backgroundBox.geometry.dispose(),this._backgroundBox.material.dispose())}_setSize(e){this._lodMax=Math.floor(Math.log2(e)),this._cubeSize=Math.pow(2,this._lodMax)}_dispose(){this._blurMaterial!==null&&this._blurMaterial.dispose(),this._ggxMaterial!==null&&this._ggxMaterial.dispose(),this._pingPongRenderTarget!==null&&this._pingPongRenderTarget.dispose();for(let e=0;e<this._lodMeshes.length;e++)this._lodMeshes[e].geometry.dispose()}_cleanup(e){this._renderer.setRenderTarget(xg,Mg,bg),this._renderer.xr.enabled=Eg,e.scissorTest=!1,Lo(e,0,0,e.width,e.height)}_fromTexture(e,t){e.mapping===Br||e.mapping===Cs?this._setSize(e.image.length===0?16:e.image[0].width||e.image[0].image.width):this._setSize(e.image.width/4),xg=this._renderer.getRenderTarget(),Mg=this._renderer.getActiveCubeFace(),bg=this._renderer.getActiveMipmapLevel(),Eg=this._renderer.xr.enabled,this._renderer.xr.enabled=!1;let i=t||this._allocateTargets();return this._textureToCubeUV(e,i),this._applyPMREM(i),this._cleanup(i),i}_allocateTargets(){let e=3*Math.max(this._cubeSize,112),t=4*this._cubeSize,i={magFilter:sn,minFilter:sn,generateMipmaps:!1,type:Ni,format:Jn,colorSpace:Es,depthBuffer:!1},r=iM(e,t,i);if(this._pingPongRenderTarget===null||this._pingPongRenderTarget.width!==e||this._pingPongRenderTarget.height!==t){this._pingPongRenderTarget!==null&&this._dispose(),this._pingPongRenderTarget=iM(e,t,i);let{_lodMax:s}=this;({lodMeshes:this._lodMeshes,sizeLods:this._sizeLods,sigmas:this._sigmas}=YA(s)),this._blurMaterial=JA(s,e,t),this._ggxMaterial=ZA(s,e,t)}return r}_compileMaterial(e){let t=new Zn(new Fn,e);this._renderer.compile(t,fc)}_sceneToCubeUV(e,t,i,r,s){let c=new hn(90,1,t,i),l=[1,-1,1,1,1,1],u=[1,1,1,-1,-1,-1],f=this._renderer,d=f.autoClear,h=f.toneMapping;f.getClearColor(nM),f.toneMapping=pi,f.autoClear=!1,f.state.buffers.depth.getReversed()&&(f.setRenderTarget(r),f.clearDepth(),f.setRenderTarget(null)),this._backgroundBox===null&&(this._backgroundBox=new Zn(new Ao,new Xa({name:"PMREM.Background",side:xn,depthWrite:!1,depthTest:!1})));let x=this._backgroundBox,m=x.material,p=!1,M=e.background;M?M.isColor&&(m.color.copy(M),e.background=null,p=!0):(m.color.copy(nM),p=!0);for(let T=0;T<6;T++){let S=T%3;S===0?(c.up.set(0,l[T],0),c.position.set(s.x,s.y,s.z),c.lookAt(s.x+u[T],s.y,s.z)):S===1?(c.up.set(0,0,l[T]),c.position.set(s.x,s.y,s.z),c.lookAt(s.x,s.y+u[T],s.z)):(c.up.set(0,l[T],0),c.position.set(s.x,s.y,s.z),c.lookAt(s.x,s.y,s.z+u[T]));let I=this._cubeSize;Lo(r,S*I,T>2?I:0,I,I),f.setRenderTarget(r),p&&f.render(x,c),f.render(e,c)}f.toneMapping=h,f.autoClear=d,e.background=M}_textureToCubeUV(e,t){let i=this._renderer,r=e.mapping===Br||e.mapping===Cs;r?(this._cubemapMaterial===null&&(this._cubemapMaterial=sM()),this._cubemapMaterial.uniforms.flipEnvMap.value=e.isRenderTargetTexture===!1?-1:1):this._equirectMaterial===null&&(this._equirectMaterial=rM());let s=r?this._cubemapMaterial:this._equirectMaterial,o=this._lodMeshes[0];o.material=s;let a=s.uniforms;a.envMap.value=e;let c=this._cubeSize;Lo(t,0,0,3*c,2*c),i.setRenderTarget(t),i.render(o,fc)}_applyPMREM(e){let t=this._renderer,i=t.autoClear;t.autoClear=!1;let r=this._lodMeshes.length;for(let s=1;s<r;s++)this._applyGGXFilter(e,s-1,s);t.autoClear=i}_applyGGXFilter(e,t,i){let r=this._renderer,s=this._pingPongRenderTarget,o=this._ggxMaterial,a=this._lodMeshes[i];a.material=o;let c=o.uniforms,l=i/(this._lodMeshes.length-1),u=t/(this._lodMeshes.length-1),f=Math.sqrt(l*l-u*u),d=0+l*1.25,h=f*d,{_lodMax:g}=this,x=this._sizeLods[i],m=3*x*(i>g-zr?i-g+zr:0),p=4*(this._cubeSize-x);c.envMap.value=e.texture,c.roughness.value=h,c.mipInt.value=g-t,Lo(s,m,p,3*x,2*x),r.setRenderTarget(s),r.render(a,fc),c.envMap.value=s.texture,c.roughness.value=0,c.mipInt.value=g-i,Lo(e,m,p,3*x,2*x),r.setRenderTarget(e),r.render(a,fc)}_blur(e,t,i,r,s){let o=this._pingPongRenderTarget;this._halfBlur(e,o,t,i,r,"latitudinal",s),this._halfBlur(o,e,i,i,r,"longitudinal",s)}_halfBlur(e,t,i,r,s,o,a){let c=this._renderer,l=this._blurMaterial;o!=="latitudinal"&&o!=="longitudinal"&&Ne("blur direction must be either latitudinal or longitudinal!");let u=3,f=this._lodMeshes[r];f.material=l;let d=l.uniforms,h=this._sizeLods[i]-1,g=isFinite(s)?Math.PI/(2*h):2*Math.PI/(2*Rs-1),x=s/g,m=isFinite(s)?1+Math.floor(u*x):Rs;m>Rs&&Le(`sigmaRadians, ${s}, is too large and will clip, as it requested ${m} samples when the maximum is set to ${Rs}`);let p=[],M=0;for(let A=0;A<Rs;++A){let y=A/x,E=Math.exp(-y*y/2);p.push(E),A===0?M+=E:A<m&&(M+=2*E)}for(let A=0;A<p.length;A++)p[A]=p[A]/M;d.envMap.value=e.texture,d.samples.value=m,d.weights.value=p,d.latitudinal.value=o==="latitudinal",a&&(d.poleAxis.value=a);let{_lodMax:T}=this;d.dTheta.value=g,d.mipInt.value=T-i;let S=this._sizeLods[r],I=3*S*(r>T-zr?r-T+zr:0),D=4*(this._cubeSize-S);Lo(t,I,D,3*S,2*S),c.setRenderTarget(t),c.render(f,fc)}};function YA(n){let e=[],t=[],i=[],r=n,s=n-zr+1+tM.length;for(let o=0;o<s;o++){let a=Math.pow(2,r);e.push(a);let c=1/a;o>n-zr?c=tM[o-n+zr-1]:o===0&&(c=0),t.push(c);let l=1/(a-2),u=-l,f=1+l,d=[u,u,f,u,f,f,u,u,f,f,u,f],h=6,g=6,x=3,m=2,p=1,M=new Float32Array(x*g*h),T=new Float32Array(m*g*h),S=new Float32Array(p*g*h);for(let D=0;D<h;D++){let A=D%3*2/3-1,y=D>2?0:-1,E=[A,y,0,A+2/3,y,0,A+2/3,y+1,0,A,y,0,A+2/3,y+1,0,A,y+1,0];M.set(E,x*g*D),T.set(d,m*g*D);let q=[D,D,D,D,D,D];S.set(q,p*g*D)}let I=new Fn;I.setAttribute("position",new pn(M,x)),I.setAttribute("uv",new pn(T,m)),I.setAttribute("faceIndex",new pn(S,p)),i.push(new Zn(I,null)),r>zr&&r--}return{lodMeshes:i,sizeLods:e,sigmas:t}}function iM(n,e,t){let i=new Ln(n,e,t);return i.texture.mapping=sc,i.texture.name="PMREM.cubeUv",i.scissorTest=!0,i}function Lo(n,e,t,i,r){n.viewport.set(e,t,i,r),n.scissor.set(e,t,i,r)}function ZA(n,e,t){return new On({name:"PMREMGGXConvolution",defines:{GGX_SAMPLES:qA,CUBEUV_TEXEL_WIDTH:1/e,CUBEUV_TEXEL_HEIGHT:1/t,CUBEUV_MAX_MIP:`${n}.0`},uniforms:{envMap:{value:null},roughness:{value:0},mipInt:{value:0}},vertexShader:Qd(),fragmentShader:`

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
		`,blending:Ri,depthTest:!1,depthWrite:!1})}function JA(n,e,t){let i=new Float32Array(Rs),r=new k(0,1,0);return new On({name:"SphericalGaussianBlur",defines:{n:Rs,CUBEUV_TEXEL_WIDTH:1/e,CUBEUV_TEXEL_HEIGHT:1/t,CUBEUV_MAX_MIP:`${n}.0`},uniforms:{envMap:{value:null},samples:{value:1},weights:{value:i},latitudinal:{value:!1},dTheta:{value:0},mipInt:{value:0},poleAxis:{value:r}},vertexShader:Qd(),fragmentShader:`

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
		`,blending:Ri,depthTest:!1,depthWrite:!1})}function rM(){return new On({name:"EquirectangularToCubeUV",uniforms:{envMap:{value:null}},vertexShader:Qd(),fragmentShader:`

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
		`,blending:Ri,depthTest:!1,depthWrite:!1})}function sM(){return new On({name:"CubemapToCubeUV",uniforms:{envMap:{value:null},flipEnvMap:{value:-1}},vertexShader:Qd(),fragmentShader:`

			precision mediump float;
			precision mediump int;

			uniform float flipEnvMap;

			varying vec3 vOutputDirection;

			uniform samplerCube envMap;

			void main() {

				gl_FragColor = textureCube( envMap, vec3( flipEnvMap * vOutputDirection.x, vOutputDirection.yz ) );

			}
		`,blending:Ri,depthTest:!1,depthWrite:!1})}function Qd(){return`

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
	`}var Jd=class extends Ln{constructor(e=1,t={}){super(e,e,t),this.isWebGLCubeRenderTarget=!0;let i={width:e,height:e,depth:1},r=[i,i,i,i,i,i];this.texture=new Ja(r),this._setTextureOptions(t),this.texture.isRenderTargetTexture=!0}fromEquirectangularTexture(e,t){this.texture.type=t.type,this.texture.colorSpace=t.colorSpace,this.texture.generateMipmaps=t.generateMipmaps,this.texture.minFilter=t.minFilter,this.texture.magFilter=t.magFilter;let i={uniforms:{tEquirect:{value:null}},vertexShader:`

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
			`},r=new Ao(5,5,5),s=new On({name:"CubemapFromEquirect",uniforms:Ds(i.uniforms),vertexShader:i.vertexShader,fragmentShader:i.fragmentShader,side:xn,blending:Ri});s.uniforms.tEquirect.value=t;let o=new Zn(r,s),a=t.minFilter;return t.minFilter===Vr&&(t.minFilter=sn),new id(1,10,this).update(e,o),t.minFilter=a,o.geometry.dispose(),o.material.dispose(),this}clear(e,t=!0,i=!0,r=!0){let s=e.getRenderTarget();for(let o=0;o<6;o++)e.setRenderTarget(this,o),e.clear(t,i,r);e.setRenderTarget(s)}};function KA(n){let e=new WeakMap,t=new WeakMap,i=null;function r(d,h=!1){return d==null?null:h?o(d):s(d)}function s(d){if(d&&d.isTexture){let h=d.mapping;if(h===sd||h===od)if(e.has(d)){let g=e.get(d).texture;return a(g,d.mapping)}else{let g=d.image;if(g&&g.height>0){let x=new Jd(g.height);return x.fromEquirectangularTexture(n,d),e.set(d,x),d.addEventListener("dispose",l),a(x.texture,d.mapping)}else return null}}return d}function o(d){if(d&&d.isTexture){let h=d.mapping,g=h===sd||h===od,x=h===Br||h===Cs;if(g||x){let m=t.get(d),p=m!==void 0?m.texture.pmremVersion:0;if(d.isRenderTargetTexture&&d.pmremVersion!==p)return i===null&&(i=new Zd(n)),m=g?i.fromEquirectangular(d,m):i.fromCubemap(d,m),m.texture.pmremVersion=d.pmremVersion,t.set(d,m),m.texture;if(m!==void 0)return m.texture;{let M=d.image;return g&&M&&M.height>0||x&&M&&c(M)?(i===null&&(i=new Zd(n)),m=g?i.fromEquirectangular(d):i.fromCubemap(d),m.texture.pmremVersion=d.pmremVersion,t.set(d,m),d.addEventListener("dispose",u),m.texture):null}}}return d}function a(d,h){return h===sd?d.mapping=Br:h===od&&(d.mapping=Cs),d}function c(d){let h=0,g=6;for(let x=0;x<g;x++)d[x]!==void 0&&h++;return h===g}function l(d){let h=d.target;h.removeEventListener("dispose",l);let g=e.get(h);g!==void 0&&(e.delete(h),g.dispose())}function u(d){let h=d.target;h.removeEventListener("dispose",u);let g=t.get(h);g!==void 0&&(t.delete(h),g.dispose())}function f(){e=new WeakMap,t=new WeakMap,i!==null&&(i.dispose(),i=null)}return{get:r,dispose:f}}function QA(n){let e={};function t(i){if(e[i]!==void 0)return e[i];let r=n.getExtension(i);return e[i]=r,r}return{has:function(i){return t(i)!==null},init:function(){t("EXT_color_buffer_float"),t("WEBGL_clip_cull_distance"),t("OES_texture_float_linear"),t("EXT_color_buffer_half_float"),t("WEBGL_multisampled_render_to_texture"),t("WEBGL_render_shared_exponent")},get:function(i){let r=t(i);return r===null&&Ha("WebGLRenderer: "+i+" extension not supported."),r}}}function e1(n,e,t,i){let r={},s=new WeakMap;function o(f){let d=f.target;d.index!==null&&e.remove(d.index);for(let g in d.attributes)e.remove(d.attributes[g]);d.removeEventListener("dispose",o),delete r[d.id];let h=s.get(d);h&&(e.remove(h),s.delete(d)),i.releaseStatesOfGeometry(d),d.isInstancedBufferGeometry===!0&&delete d._maxInstanceCount,t.memory.geometries--}function a(f,d){return r[d.id]===!0||(d.addEventListener("dispose",o),r[d.id]=!0,t.memory.geometries++),d}function c(f){let d=f.attributes;for(let h in d)e.update(d[h],n.ARRAY_BUFFER)}function l(f){let d=[],h=f.index,g=f.attributes.position,x=0;if(g===void 0)return;if(h!==null){let M=h.array;x=h.version;for(let T=0,S=M.length;T<S;T+=3){let I=M[T+0],D=M[T+1],A=M[T+2];d.push(I,D,D,A,A,I)}}else{let M=g.array;x=g.version;for(let T=0,S=M.length/3-1;T<S;T+=3){let I=T+0,D=T+1,A=T+2;d.push(I,D,D,A,A,I)}}let m=new(g.count>=65535?$a:ja)(d,1);m.version=x;let p=s.get(f);p&&e.remove(p),s.set(f,m)}function u(f){let d=s.get(f);if(d){let h=f.index;h!==null&&d.version<h.version&&l(f)}else l(f);return s.get(f)}return{get:a,update:c,getWireframeAttribute:u}}function t1(n,e,t){let i;function r(d){i=d}let s,o;function a(d){s=d.type,o=d.bytesPerElement}function c(d,h){n.drawElements(i,h,s,d*o),t.update(h,i,1)}function l(d,h,g){g!==0&&(n.drawElementsInstanced(i,h,s,d*o,g),t.update(h,i,g))}function u(d,h,g){if(g===0)return;e.get("WEBGL_multi_draw").multiDrawElementsWEBGL(i,h,0,s,d,0,g);let m=0;for(let p=0;p<g;p++)m+=h[p];t.update(m,i,1)}function f(d,h,g,x){if(g===0)return;let m=e.get("WEBGL_multi_draw");if(m===null)for(let p=0;p<d.length;p++)l(d[p]/o,h[p],x[p]);else{m.multiDrawElementsInstancedWEBGL(i,h,0,s,d,0,x,0,g);let p=0;for(let M=0;M<g;M++)p+=h[M]*x[M];t.update(p,i,1)}}this.setMode=r,this.setIndex=a,this.render=c,this.renderInstances=l,this.renderMultiDraw=u,this.renderMultiDrawInstances=f}function n1(n){let e={geometries:0,textures:0},t={frame:0,calls:0,triangles:0,points:0,lines:0};function i(s,o,a){switch(t.calls++,o){case n.TRIANGLES:t.triangles+=a*(s/3);break;case n.LINES:t.lines+=a*(s/2);break;case n.LINE_STRIP:t.lines+=a*(s-1);break;case n.LINE_LOOP:t.lines+=a*s;break;case n.POINTS:t.points+=a*s;break;default:Ne("WebGLInfo: Unknown draw mode:",o);break}}function r(){t.calls=0,t.triangles=0,t.points=0,t.lines=0}return{memory:e,render:t,programs:null,autoReset:!0,reset:r,update:i}}function i1(n,e,t){let i=new WeakMap,r=new Lt;function s(o,a,c){let l=o.morphTargetInfluences,u=a.morphAttributes.position||a.morphAttributes.normal||a.morphAttributes.color,f=u!==void 0?u.length:0,d=i.get(a);if(d===void 0||d.count!==f){let q=function(){y.dispose(),i.delete(a),a.removeEventListener("dispose",q)};var h=q;d!==void 0&&d.texture.dispose();let g=a.morphAttributes.position!==void 0,x=a.morphAttributes.normal!==void 0,m=a.morphAttributes.color!==void 0,p=a.morphAttributes.position||[],M=a.morphAttributes.normal||[],T=a.morphAttributes.color||[],S=0;g===!0&&(S=1),x===!0&&(S=2),m===!0&&(S=3);let I=a.attributes.position.count*S,D=1;I>e.maxTextureSize&&(D=Math.ceil(I/e.maxTextureSize),I=e.maxTextureSize);let A=new Float32Array(I*D*4*f),y=new za(A,I,D,f);y.type=gi,y.needsUpdate=!0;let E=S*4;for(let C=0;C<f;C++){let U=p[C],V=M[C],W=T[C],B=I*D*4*C;for(let H=0;H<U.count;H++){let L=H*E;g===!0&&(r.fromBufferAttribute(U,H),A[B+L+0]=r.x,A[B+L+1]=r.y,A[B+L+2]=r.z,A[B+L+3]=0),x===!0&&(r.fromBufferAttribute(V,H),A[B+L+4]=r.x,A[B+L+5]=r.y,A[B+L+6]=r.z,A[B+L+7]=0),m===!0&&(r.fromBufferAttribute(W,H),A[B+L+8]=r.x,A[B+L+9]=r.y,A[B+L+10]=r.z,A[B+L+11]=W.itemSize===4?r.w:1)}}d={count:f,texture:y,size:new pt(I,D)},i.set(a,d),a.addEventListener("dispose",q)}if(o.isInstancedMesh===!0&&o.morphTexture!==null)c.getUniforms().setValue(n,"morphTexture",o.morphTexture,t);else{let g=0;for(let m=0;m<l.length;m++)g+=l[m];let x=a.morphTargetsRelative?1:1-g;c.getUniforms().setValue(n,"morphTargetBaseInfluence",x),c.getUniforms().setValue(n,"morphTargetInfluences",l)}c.getUniforms().setValue(n,"morphTargetsTexture",d.texture,t),c.getUniforms().setValue(n,"morphTargetsTextureSize",d.size)}return{update:s}}function r1(n,e,t,i,r){let s=new WeakMap;function o(l){let u=r.render.frame,f=l.geometry,d=e.get(l,f);if(s.get(d)!==u&&(e.update(d),s.set(d,u)),l.isInstancedMesh&&(l.hasEventListener("dispose",c)===!1&&l.addEventListener("dispose",c),s.get(l)!==u&&(t.update(l.instanceMatrix,n.ARRAY_BUFFER),l.instanceColor!==null&&t.update(l.instanceColor,n.ARRAY_BUFFER),s.set(l,u))),l.isSkinnedMesh){let h=l.skeleton;s.get(h)!==u&&(h.update(),s.set(h,u))}return d}function a(){s=new WeakMap}function c(l){let u=l.target;u.removeEventListener("dispose",c),i.releaseStatesOfObject(u),t.remove(u.instanceMatrix),u.instanceColor!==null&&t.remove(u.instanceColor)}return{update:o,dispose:a}}var s1={[eg]:"LINEAR_TONE_MAPPING",[tg]:"REINHARD_TONE_MAPPING",[ng]:"CINEON_TONE_MAPPING",[ig]:"ACES_FILMIC_TONE_MAPPING",[sg]:"AGX_TONE_MAPPING",[og]:"NEUTRAL_TONE_MAPPING",[rg]:"CUSTOM_TONE_MAPPING"};function o1(n,e,t,i,r){let s=new Ln(e,t,{type:n,depthBuffer:i,stencilBuffer:r}),o=new Ln(e,t,{type:Ni,depthBuffer:!1,stencilBuffer:!1}),a=new Fn;a.setAttribute("position",new Yn([-1,3,0,-1,-1,0,3,-1,0],3)),a.setAttribute("uv",new Yn([0,2,0,0,2,0],2));let c=new ju({uniforms:{tDiffuse:{value:null}},vertexShader:`
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
			}`,depthTest:!1,depthWrite:!1}),l=new Zn(a,c),u=new nc(-1,1,1,-1,0,1),f=null,d=null,h=!1,g,x=null,m=[],p=!1;this.setSize=function(M,T){s.setSize(M,T),o.setSize(M,T);for(let S=0;S<m.length;S++){let I=m[S];I.setSize&&I.setSize(M,T)}},this.setEffects=function(M){m=M,p=m.length>0&&m[0].isRenderPass===!0;let T=s.width,S=s.height;for(let I=0;I<m.length;I++){let D=m[I];D.setSize&&D.setSize(T,S)}},this.begin=function(M,T){if(h||M.toneMapping===pi&&m.length===0)return!1;if(x=T,T!==null){let S=T.width,I=T.height;(s.width!==S||s.height!==I)&&this.setSize(S,I)}return p===!1&&M.setRenderTarget(s),g=M.toneMapping,M.toneMapping=pi,!0},this.hasRenderPass=function(){return p},this.end=function(M,T){M.toneMapping=g,h=!0;let S=s,I=o;for(let D=0;D<m.length;D++){let A=m[D];if(A.enabled!==!1&&(A.render(M,I,S,T),A.needsSwap!==!1)){let y=S;S=I,I=y}}if(f!==M.outputColorSpace||d!==M.toneMapping){f=M.outputColorSpace,d=M.toneMapping,c.defines={},rt.getTransfer(f)===dt&&(c.defines.SRGB_TRANSFER="");let D=s1[d];D&&(c.defines[D]=""),c.needsUpdate=!0}c.uniforms.tDiffuse.value=S.texture,M.setRenderTarget(x),M.render(l,u),x=null,h=!1},this.isCompositing=function(){return h},this.dispose=function(){s.dispose(),o.dispose(),a.dispose(),c.dispose()}}var SM=new ar,Cg=new Fr(1,1),TM=new za,CM=new zu,wM=new Ja,oM=[],aM=[],cM=new Float32Array(16),lM=new Float32Array(9),uM=new Float32Array(4);function Oo(n,e,t){let i=n[0];if(i<=0||i>0)return n;let r=e*t,s=oM[r];if(s===void 0&&(s=new Float32Array(r),oM[r]=s),e!==0){i.toArray(s,0);for(let o=1,a=0;o!==e;++o)a+=t,n[o].toArray(s,a)}return s}function qt(n,e){if(n.length!==e.length)return!1;for(let t=0,i=n.length;t<i;t++)if(n[t]!==e[t])return!1;return!0}function Xt(n,e){for(let t=0,i=e.length;t<i;t++)n[t]=e[t]}function ef(n,e){let t=aM[e];t===void 0&&(t=new Int32Array(e),aM[e]=t);for(let i=0;i!==e;++i)t[i]=n.allocateTextureUnit();return t}function a1(n,e){let t=this.cache;t[0]!==e&&(n.uniform1f(this.addr,e),t[0]=e)}function c1(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y)&&(n.uniform2f(this.addr,e.x,e.y),t[0]=e.x,t[1]=e.y);else{if(qt(t,e))return;n.uniform2fv(this.addr,e),Xt(t,e)}}function l1(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y||t[2]!==e.z)&&(n.uniform3f(this.addr,e.x,e.y,e.z),t[0]=e.x,t[1]=e.y,t[2]=e.z);else if(e.r!==void 0)(t[0]!==e.r||t[1]!==e.g||t[2]!==e.b)&&(n.uniform3f(this.addr,e.r,e.g,e.b),t[0]=e.r,t[1]=e.g,t[2]=e.b);else{if(qt(t,e))return;n.uniform3fv(this.addr,e),Xt(t,e)}}function u1(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y||t[2]!==e.z||t[3]!==e.w)&&(n.uniform4f(this.addr,e.x,e.y,e.z,e.w),t[0]=e.x,t[1]=e.y,t[2]=e.z,t[3]=e.w);else{if(qt(t,e))return;n.uniform4fv(this.addr,e),Xt(t,e)}}function d1(n,e){let t=this.cache,i=e.elements;if(i===void 0){if(qt(t,e))return;n.uniformMatrix2fv(this.addr,!1,e),Xt(t,e)}else{if(qt(t,i))return;uM.set(i),n.uniformMatrix2fv(this.addr,!1,uM),Xt(t,i)}}function f1(n,e){let t=this.cache,i=e.elements;if(i===void 0){if(qt(t,e))return;n.uniformMatrix3fv(this.addr,!1,e),Xt(t,e)}else{if(qt(t,i))return;lM.set(i),n.uniformMatrix3fv(this.addr,!1,lM),Xt(t,i)}}function h1(n,e){let t=this.cache,i=e.elements;if(i===void 0){if(qt(t,e))return;n.uniformMatrix4fv(this.addr,!1,e),Xt(t,e)}else{if(qt(t,i))return;cM.set(i),n.uniformMatrix4fv(this.addr,!1,cM),Xt(t,i)}}function p1(n,e){let t=this.cache;t[0]!==e&&(n.uniform1i(this.addr,e),t[0]=e)}function m1(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y)&&(n.uniform2i(this.addr,e.x,e.y),t[0]=e.x,t[1]=e.y);else{if(qt(t,e))return;n.uniform2iv(this.addr,e),Xt(t,e)}}function g1(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y||t[2]!==e.z)&&(n.uniform3i(this.addr,e.x,e.y,e.z),t[0]=e.x,t[1]=e.y,t[2]=e.z);else{if(qt(t,e))return;n.uniform3iv(this.addr,e),Xt(t,e)}}function v1(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y||t[2]!==e.z||t[3]!==e.w)&&(n.uniform4i(this.addr,e.x,e.y,e.z,e.w),t[0]=e.x,t[1]=e.y,t[2]=e.z,t[3]=e.w);else{if(qt(t,e))return;n.uniform4iv(this.addr,e),Xt(t,e)}}function y1(n,e){let t=this.cache;t[0]!==e&&(n.uniform1ui(this.addr,e),t[0]=e)}function _1(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y)&&(n.uniform2ui(this.addr,e.x,e.y),t[0]=e.x,t[1]=e.y);else{if(qt(t,e))return;n.uniform2uiv(this.addr,e),Xt(t,e)}}function x1(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y||t[2]!==e.z)&&(n.uniform3ui(this.addr,e.x,e.y,e.z),t[0]=e.x,t[1]=e.y,t[2]=e.z);else{if(qt(t,e))return;n.uniform3uiv(this.addr,e),Xt(t,e)}}function M1(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y||t[2]!==e.z||t[3]!==e.w)&&(n.uniform4ui(this.addr,e.x,e.y,e.z,e.w),t[0]=e.x,t[1]=e.y,t[2]=e.z,t[3]=e.w);else{if(qt(t,e))return;n.uniform4uiv(this.addr,e),Xt(t,e)}}function b1(n,e,t){let i=this.cache,r=t.allocateTextureUnit();i[0]!==r&&(n.uniform1i(this.addr,r),i[0]=r);let s;this.type===n.SAMPLER_2D_SHADOW?(Cg.compareFunction=t.isReversedDepthBuffer()?qd:$d,s=Cg):s=SM,t.setTexture2D(e||s,r)}function E1(n,e,t){let i=this.cache,r=t.allocateTextureUnit();i[0]!==r&&(n.uniform1i(this.addr,r),i[0]=r),t.setTexture3D(e||CM,r)}function S1(n,e,t){let i=this.cache,r=t.allocateTextureUnit();i[0]!==r&&(n.uniform1i(this.addr,r),i[0]=r),t.setTextureCube(e||wM,r)}function T1(n,e,t){let i=this.cache,r=t.allocateTextureUnit();i[0]!==r&&(n.uniform1i(this.addr,r),i[0]=r),t.setTexture2DArray(e||TM,r)}function C1(n){switch(n){case 5126:return a1;case 35664:return c1;case 35665:return l1;case 35666:return u1;case 35674:return d1;case 35675:return f1;case 35676:return h1;case 5124:case 35670:return p1;case 35667:case 35671:return m1;case 35668:case 35672:return g1;case 35669:case 35673:return v1;case 5125:return y1;case 36294:return _1;case 36295:return x1;case 36296:return M1;case 35678:case 36198:case 36298:case 36306:case 35682:return b1;case 35679:case 36299:case 36307:return E1;case 35680:case 36300:case 36308:case 36293:return S1;case 36289:case 36303:case 36311:case 36292:return T1}}function w1(n,e){n.uniform1fv(this.addr,e)}function D1(n,e){let t=Oo(e,this.size,2);n.uniform2fv(this.addr,t)}function I1(n,e){let t=Oo(e,this.size,3);n.uniform3fv(this.addr,t)}function A1(n,e){let t=Oo(e,this.size,4);n.uniform4fv(this.addr,t)}function R1(n,e){let t=Oo(e,this.size,4);n.uniformMatrix2fv(this.addr,!1,t)}function N1(n,e){let t=Oo(e,this.size,9);n.uniformMatrix3fv(this.addr,!1,t)}function P1(n,e){let t=Oo(e,this.size,16);n.uniformMatrix4fv(this.addr,!1,t)}function L1(n,e){n.uniform1iv(this.addr,e)}function F1(n,e){n.uniform2iv(this.addr,e)}function O1(n,e){n.uniform3iv(this.addr,e)}function k1(n,e){n.uniform4iv(this.addr,e)}function U1(n,e){n.uniform1uiv(this.addr,e)}function B1(n,e){n.uniform2uiv(this.addr,e)}function V1(n,e){n.uniform3uiv(this.addr,e)}function H1(n,e){n.uniform4uiv(this.addr,e)}function z1(n,e,t){let i=this.cache,r=e.length,s=ef(t,r);qt(i,s)||(n.uniform1iv(this.addr,s),Xt(i,s));let o;this.type===n.SAMPLER_2D_SHADOW?o=Cg:o=SM;for(let a=0;a!==r;++a)t.setTexture2D(e[a]||o,s[a])}function G1(n,e,t){let i=this.cache,r=e.length,s=ef(t,r);qt(i,s)||(n.uniform1iv(this.addr,s),Xt(i,s));for(let o=0;o!==r;++o)t.setTexture3D(e[o]||CM,s[o])}function W1(n,e,t){let i=this.cache,r=e.length,s=ef(t,r);qt(i,s)||(n.uniform1iv(this.addr,s),Xt(i,s));for(let o=0;o!==r;++o)t.setTextureCube(e[o]||wM,s[o])}function j1(n,e,t){let i=this.cache,r=e.length,s=ef(t,r);qt(i,s)||(n.uniform1iv(this.addr,s),Xt(i,s));for(let o=0;o!==r;++o)t.setTexture2DArray(e[o]||TM,s[o])}function $1(n){switch(n){case 5126:return w1;case 35664:return D1;case 35665:return I1;case 35666:return A1;case 35674:return R1;case 35675:return N1;case 35676:return P1;case 5124:case 35670:return L1;case 35667:case 35671:return F1;case 35668:case 35672:return O1;case 35669:case 35673:return k1;case 5125:return U1;case 36294:return B1;case 36295:return V1;case 36296:return H1;case 35678:case 36198:case 36298:case 36306:case 35682:return z1;case 35679:case 36299:case 36307:return G1;case 35680:case 36300:case 36308:case 36293:return W1;case 36289:case 36303:case 36311:case 36292:return j1}}var wg=class{constructor(e,t,i){this.id=e,this.addr=i,this.cache=[],this.type=t.type,this.setValue=C1(t.type)}},Dg=class{constructor(e,t,i){this.id=e,this.addr=i,this.cache=[],this.type=t.type,this.size=t.size,this.setValue=$1(t.type)}},Ig=class{constructor(e){this.id=e,this.seq=[],this.map={}}setValue(e,t,i){let r=this.seq;for(let s=0,o=r.length;s!==o;++s){let a=r[s];a.setValue(e,t[a.id],i)}}},Sg=/(\w+)(\])?(\[|\.)?/g;function dM(n,e){n.seq.push(e),n.map[e.id]=e}function q1(n,e,t){let i=n.name,r=i.length;for(Sg.lastIndex=0;;){let s=Sg.exec(i),o=Sg.lastIndex,a=s[1],c=s[2]==="]",l=s[3];if(c&&(a=a|0),l===void 0||l==="["&&o+2===r){dM(t,l===void 0?new wg(a,n,e):new Dg(a,n,e));break}else{let f=t.map[a];f===void 0&&(f=new Ig(a),dM(t,f)),t=f}}}var Fo=class{constructor(e,t){this.seq=[],this.map={};let i=e.getProgramParameter(t,e.ACTIVE_UNIFORMS);for(let o=0;o<i;++o){let a=e.getActiveUniform(t,o),c=e.getUniformLocation(t,a.name);q1(a,c,this)}let r=[],s=[];for(let o of this.seq)o.type===e.SAMPLER_2D_SHADOW||o.type===e.SAMPLER_CUBE_SHADOW||o.type===e.SAMPLER_2D_ARRAY_SHADOW?r.push(o):s.push(o);r.length>0&&(this.seq=r.concat(s))}setValue(e,t,i,r){let s=this.map[t];s!==void 0&&s.setValue(e,i,r)}setOptional(e,t,i){let r=t[i];r!==void 0&&this.setValue(e,i,r)}static upload(e,t,i,r){for(let s=0,o=t.length;s!==o;++s){let a=t[s],c=i[a.id];c.needsUpdate!==!1&&a.setValue(e,c.value,r)}}static seqWithValue(e,t){let i=[];for(let r=0,s=e.length;r!==s;++r){let o=e[r];o.id in t&&i.push(o)}return i}};function fM(n,e,t){let i=n.createShader(e);return n.shaderSource(i,t),n.compileShader(i),i}var X1=37297,Y1=0;function Z1(n,e){let t=n.split(`
`),i=[],r=Math.max(e-6,0),s=Math.min(e+6,t.length);for(let o=r;o<s;o++){let a=o+1;i.push(`${a===e?">":" "} ${a}: ${t[o]}`)}return i.join(`
`)}var hM=new Be;function J1(n){rt._getMatrix(hM,rt.workingColorSpace,n);let e=`mat3( ${hM.elements.map(t=>t.toFixed(4))} )`;switch(rt.getTransfer(n)){case Ua:return[e,"LinearTransferOETF"];case dt:return[e,"sRGBTransferOETF"];default:return Le("WebGLProgram: Unsupported color space: ",n),[e,"LinearTransferOETF"]}}function pM(n,e,t){let i=n.getShaderParameter(e,n.COMPILE_STATUS),s=(n.getShaderInfoLog(e)||"").trim();if(i&&s==="")return"";let o=/ERROR: 0:(\d+)/.exec(s);if(o){let a=parseInt(o[1]);return t.toUpperCase()+`

`+s+`

`+Z1(n.getShaderSource(e),a)}else return s}function K1(n,e){let t=J1(e);return[`vec4 ${n}( vec4 value ) {`,`	return ${t[1]}( vec4( value.rgb * ${t[0]}, value.a ) );`,"}"].join(`
`)}var Q1={[eg]:"Linear",[tg]:"Reinhard",[ng]:"Cineon",[ig]:"ACESFilmic",[sg]:"AgX",[og]:"Neutral",[rg]:"Custom"};function eR(n,e){let t=Q1[e];return t===void 0?(Le("WebGLProgram: Unsupported toneMapping:",e),"vec3 "+n+"( vec3 color ) { return LinearToneMapping( color ); }"):"vec3 "+n+"( vec3 color ) { return "+t+"ToneMapping( color ); }"}var Yd=new k;function tR(){rt.getLuminanceCoefficients(Yd);let n=Yd.x.toFixed(4),e=Yd.y.toFixed(4),t=Yd.z.toFixed(4);return["float luminance( const in vec3 rgb ) {",`	const vec3 weights = vec3( ${n}, ${e}, ${t} );`,"	return dot( weights, rgb );","}"].join(`
`)}function nR(n){return[n.extensionClipCullDistance?"#extension GL_ANGLE_clip_cull_distance : require":"",n.extensionMultiDraw?"#extension GL_ANGLE_multi_draw : require":""].filter(pc).join(`
`)}function iR(n){let e=[];for(let t in n){let i=n[t];i!==!1&&e.push("#define "+t+" "+i)}return e.join(`
`)}function rR(n,e){let t={},i=n.getProgramParameter(e,n.ACTIVE_ATTRIBUTES);for(let r=0;r<i;r++){let s=n.getActiveAttrib(e,r),o=s.name,a=1;s.type===n.FLOAT_MAT2&&(a=2),s.type===n.FLOAT_MAT3&&(a=3),s.type===n.FLOAT_MAT4&&(a=4),t[o]={type:s.type,location:n.getAttribLocation(e,o),locationSize:a}}return t}function pc(n){return n!==""}function mM(n,e){let t=e.numSpotLightShadows+e.numSpotLightMaps-e.numSpotLightShadowsWithMaps;return n.replace(/NUM_DIR_LIGHTS/g,e.numDirLights).replace(/NUM_SPOT_LIGHTS/g,e.numSpotLights).replace(/NUM_SPOT_LIGHT_MAPS/g,e.numSpotLightMaps).replace(/NUM_SPOT_LIGHT_COORDS/g,t).replace(/NUM_RECT_AREA_LIGHTS/g,e.numRectAreaLights).replace(/NUM_POINT_LIGHTS/g,e.numPointLights).replace(/NUM_HEMI_LIGHTS/g,e.numHemiLights).replace(/NUM_DIR_LIGHT_SHADOWS/g,e.numDirLightShadows).replace(/NUM_SPOT_LIGHT_SHADOWS_WITH_MAPS/g,e.numSpotLightShadowsWithMaps).replace(/NUM_SPOT_LIGHT_SHADOWS/g,e.numSpotLightShadows).replace(/NUM_POINT_LIGHT_SHADOWS/g,e.numPointLightShadows)}function gM(n,e){return n.replace(/NUM_CLIPPING_PLANES/g,e.numClippingPlanes).replace(/UNION_CLIPPING_PLANES/g,e.numClippingPlanes-e.numClipIntersection)}var sR=/^[ \t]*#include +<([\w\d./]+)>/gm;function Ag(n){return n.replace(sR,aR)}var oR=new Map;function aR(n,e){let t=Ge[e];if(t===void 0){let i=oR.get(e);if(i!==void 0)t=Ge[i],Le('WebGLRenderer: Shader chunk "%s" has been deprecated. Use "%s" instead.',e,i);else throw new Error("Can not resolve #include <"+e+">")}return Ag(t)}var cR=/#pragma unroll_loop_start\s+for\s*\(\s*int\s+i\s*=\s*(\d+)\s*;\s*i\s*<\s*(\d+)\s*;\s*i\s*\+\+\s*\)\s*{([\s\S]+?)}\s+#pragma unroll_loop_end/g;function vM(n){return n.replace(cR,lR)}function lR(n,e,t,i){let r="";for(let s=parseInt(e);s<parseInt(t);s++)r+=i.replace(/\[\s*i\s*\]/g,"[ "+s+" ]").replace(/UNROLLED_LOOP_INDEX/g,s);return r}function yM(n){let e=`precision ${n.precision} float;
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
#define LOW_PRECISION`),e}var uR={[rc]:"SHADOWMAP_TYPE_PCF",[Ro]:"SHADOWMAP_TYPE_VSM"};function dR(n){return uR[n.shadowMapType]||"SHADOWMAP_TYPE_BASIC"}var fR={[Br]:"ENVMAP_TYPE_CUBE",[Cs]:"ENVMAP_TYPE_CUBE",[sc]:"ENVMAP_TYPE_CUBE_UV"};function hR(n){return n.envMap===!1?"ENVMAP_TYPE_CUBE":fR[n.envMapMode]||"ENVMAP_TYPE_CUBE"}var pR={[Cs]:"ENVMAP_MODE_REFRACTION"};function mR(n){return n.envMap===!1?"ENVMAP_MODE_REFLECTION":pR[n.envMapMode]||"ENVMAP_MODE_REFLECTION"}var gR={[Qm]:"ENVMAP_BLENDING_MULTIPLY",[kx]:"ENVMAP_BLENDING_MIX",[Ux]:"ENVMAP_BLENDING_ADD"};function vR(n){return n.envMap===!1?"ENVMAP_BLENDING_NONE":gR[n.combine]||"ENVMAP_BLENDING_NONE"}function yR(n){let e=n.envMapCubeUVHeight;if(e===null)return null;let t=Math.log2(e)-2,i=1/e;return{texelWidth:1/(3*Math.max(Math.pow(2,t),112)),texelHeight:i,maxMip:t}}function _R(n,e,t,i){let r=n.getContext(),s=t.defines,o=t.vertexShader,a=t.fragmentShader,c=dR(t),l=hR(t),u=mR(t),f=vR(t),d=yR(t),h=nR(t),g=iR(s),x=r.createProgram(),m,p,M=t.glslVersion?"#version "+t.glslVersion+`
`:"";t.isRawShaderMaterial?(m=["#define SHADER_TYPE "+t.shaderType,"#define SHADER_NAME "+t.shaderName,g].filter(pc).join(`
`),m.length>0&&(m+=`
`),p=["#define SHADER_TYPE "+t.shaderType,"#define SHADER_NAME "+t.shaderName,g].filter(pc).join(`
`),p.length>0&&(p+=`
`)):(m=[yM(t),"#define SHADER_TYPE "+t.shaderType,"#define SHADER_NAME "+t.shaderName,g,t.extensionClipCullDistance?"#define USE_CLIP_DISTANCE":"",t.batching?"#define USE_BATCHING":"",t.batchingColor?"#define USE_BATCHING_COLOR":"",t.instancing?"#define USE_INSTANCING":"",t.instancingColor?"#define USE_INSTANCING_COLOR":"",t.instancingMorph?"#define USE_INSTANCING_MORPH":"",t.useFog&&t.fog?"#define USE_FOG":"",t.useFog&&t.fogExp2?"#define FOG_EXP2":"",t.map?"#define USE_MAP":"",t.envMap?"#define USE_ENVMAP":"",t.envMap?"#define "+u:"",t.lightMap?"#define USE_LIGHTMAP":"",t.aoMap?"#define USE_AOMAP":"",t.bumpMap?"#define USE_BUMPMAP":"",t.normalMap?"#define USE_NORMALMAP":"",t.normalMapObjectSpace?"#define USE_NORMALMAP_OBJECTSPACE":"",t.normalMapTangentSpace?"#define USE_NORMALMAP_TANGENTSPACE":"",t.displacementMap?"#define USE_DISPLACEMENTMAP":"",t.emissiveMap?"#define USE_EMISSIVEMAP":"",t.anisotropy?"#define USE_ANISOTROPY":"",t.anisotropyMap?"#define USE_ANISOTROPYMAP":"",t.clearcoatMap?"#define USE_CLEARCOATMAP":"",t.clearcoatRoughnessMap?"#define USE_CLEARCOAT_ROUGHNESSMAP":"",t.clearcoatNormalMap?"#define USE_CLEARCOAT_NORMALMAP":"",t.iridescenceMap?"#define USE_IRIDESCENCEMAP":"",t.iridescenceThicknessMap?"#define USE_IRIDESCENCE_THICKNESSMAP":"",t.specularMap?"#define USE_SPECULARMAP":"",t.specularColorMap?"#define USE_SPECULAR_COLORMAP":"",t.specularIntensityMap?"#define USE_SPECULAR_INTENSITYMAP":"",t.roughnessMap?"#define USE_ROUGHNESSMAP":"",t.metalnessMap?"#define USE_METALNESSMAP":"",t.alphaMap?"#define USE_ALPHAMAP":"",t.alphaHash?"#define USE_ALPHAHASH":"",t.transmission?"#define USE_TRANSMISSION":"",t.transmissionMap?"#define USE_TRANSMISSIONMAP":"",t.thicknessMap?"#define USE_THICKNESSMAP":"",t.sheenColorMap?"#define USE_SHEEN_COLORMAP":"",t.sheenRoughnessMap?"#define USE_SHEEN_ROUGHNESSMAP":"",t.mapUv?"#define MAP_UV "+t.mapUv:"",t.alphaMapUv?"#define ALPHAMAP_UV "+t.alphaMapUv:"",t.lightMapUv?"#define LIGHTMAP_UV "+t.lightMapUv:"",t.aoMapUv?"#define AOMAP_UV "+t.aoMapUv:"",t.emissiveMapUv?"#define EMISSIVEMAP_UV "+t.emissiveMapUv:"",t.bumpMapUv?"#define BUMPMAP_UV "+t.bumpMapUv:"",t.normalMapUv?"#define NORMALMAP_UV "+t.normalMapUv:"",t.displacementMapUv?"#define DISPLACEMENTMAP_UV "+t.displacementMapUv:"",t.metalnessMapUv?"#define METALNESSMAP_UV "+t.metalnessMapUv:"",t.roughnessMapUv?"#define ROUGHNESSMAP_UV "+t.roughnessMapUv:"",t.anisotropyMapUv?"#define ANISOTROPYMAP_UV "+t.anisotropyMapUv:"",t.clearcoatMapUv?"#define CLEARCOATMAP_UV "+t.clearcoatMapUv:"",t.clearcoatNormalMapUv?"#define CLEARCOAT_NORMALMAP_UV "+t.clearcoatNormalMapUv:"",t.clearcoatRoughnessMapUv?"#define CLEARCOAT_ROUGHNESSMAP_UV "+t.clearcoatRoughnessMapUv:"",t.iridescenceMapUv?"#define IRIDESCENCEMAP_UV "+t.iridescenceMapUv:"",t.iridescenceThicknessMapUv?"#define IRIDESCENCE_THICKNESSMAP_UV "+t.iridescenceThicknessMapUv:"",t.sheenColorMapUv?"#define SHEEN_COLORMAP_UV "+t.sheenColorMapUv:"",t.sheenRoughnessMapUv?"#define SHEEN_ROUGHNESSMAP_UV "+t.sheenRoughnessMapUv:"",t.specularMapUv?"#define SPECULARMAP_UV "+t.specularMapUv:"",t.specularColorMapUv?"#define SPECULAR_COLORMAP_UV "+t.specularColorMapUv:"",t.specularIntensityMapUv?"#define SPECULAR_INTENSITYMAP_UV "+t.specularIntensityMapUv:"",t.transmissionMapUv?"#define TRANSMISSIONMAP_UV "+t.transmissionMapUv:"",t.thicknessMapUv?"#define THICKNESSMAP_UV "+t.thicknessMapUv:"",t.vertexTangents&&t.flatShading===!1?"#define USE_TANGENT":"",t.vertexColors?"#define USE_COLOR":"",t.vertexAlphas?"#define USE_COLOR_ALPHA":"",t.vertexUv1s?"#define USE_UV1":"",t.vertexUv2s?"#define USE_UV2":"",t.vertexUv3s?"#define USE_UV3":"",t.pointsUvs?"#define USE_POINTS_UV":"",t.flatShading?"#define FLAT_SHADED":"",t.skinning?"#define USE_SKINNING":"",t.morphTargets?"#define USE_MORPHTARGETS":"",t.morphNormals&&t.flatShading===!1?"#define USE_MORPHNORMALS":"",t.morphColors?"#define USE_MORPHCOLORS":"",t.morphTargetsCount>0?"#define MORPHTARGETS_TEXTURE_STRIDE "+t.morphTextureStride:"",t.morphTargetsCount>0?"#define MORPHTARGETS_COUNT "+t.morphTargetsCount:"",t.doubleSided?"#define DOUBLE_SIDED":"",t.flipSided?"#define FLIP_SIDED":"",t.shadowMapEnabled?"#define USE_SHADOWMAP":"",t.shadowMapEnabled?"#define "+c:"",t.sizeAttenuation?"#define USE_SIZEATTENUATION":"",t.numLightProbes>0?"#define USE_LIGHT_PROBES":"",t.logarithmicDepthBuffer?"#define USE_LOGARITHMIC_DEPTH_BUFFER":"",t.reversedDepthBuffer?"#define USE_REVERSED_DEPTH_BUFFER":"","uniform mat4 modelMatrix;","uniform mat4 modelViewMatrix;","uniform mat4 projectionMatrix;","uniform mat4 viewMatrix;","uniform mat3 normalMatrix;","uniform vec3 cameraPosition;","uniform bool isOrthographic;","#ifdef USE_INSTANCING","	attribute mat4 instanceMatrix;","#endif","#ifdef USE_INSTANCING_COLOR","	attribute vec3 instanceColor;","#endif","#ifdef USE_INSTANCING_MORPH","	uniform sampler2D morphTexture;","#endif","attribute vec3 position;","attribute vec3 normal;","attribute vec2 uv;","#ifdef USE_UV1","	attribute vec2 uv1;","#endif","#ifdef USE_UV2","	attribute vec2 uv2;","#endif","#ifdef USE_UV3","	attribute vec2 uv3;","#endif","#ifdef USE_TANGENT","	attribute vec4 tangent;","#endif","#if defined( USE_COLOR_ALPHA )","	attribute vec4 color;","#elif defined( USE_COLOR )","	attribute vec3 color;","#endif","#ifdef USE_SKINNING","	attribute vec4 skinIndex;","	attribute vec4 skinWeight;","#endif",`
`].filter(pc).join(`
`),p=[yM(t),"#define SHADER_TYPE "+t.shaderType,"#define SHADER_NAME "+t.shaderName,g,t.useFog&&t.fog?"#define USE_FOG":"",t.useFog&&t.fogExp2?"#define FOG_EXP2":"",t.alphaToCoverage?"#define ALPHA_TO_COVERAGE":"",t.map?"#define USE_MAP":"",t.matcap?"#define USE_MATCAP":"",t.envMap?"#define USE_ENVMAP":"",t.envMap?"#define "+l:"",t.envMap?"#define "+u:"",t.envMap?"#define "+f:"",d?"#define CUBEUV_TEXEL_WIDTH "+d.texelWidth:"",d?"#define CUBEUV_TEXEL_HEIGHT "+d.texelHeight:"",d?"#define CUBEUV_MAX_MIP "+d.maxMip+".0":"",t.lightMap?"#define USE_LIGHTMAP":"",t.aoMap?"#define USE_AOMAP":"",t.bumpMap?"#define USE_BUMPMAP":"",t.normalMap?"#define USE_NORMALMAP":"",t.normalMapObjectSpace?"#define USE_NORMALMAP_OBJECTSPACE":"",t.normalMapTangentSpace?"#define USE_NORMALMAP_TANGENTSPACE":"",t.emissiveMap?"#define USE_EMISSIVEMAP":"",t.anisotropy?"#define USE_ANISOTROPY":"",t.anisotropyMap?"#define USE_ANISOTROPYMAP":"",t.clearcoat?"#define USE_CLEARCOAT":"",t.clearcoatMap?"#define USE_CLEARCOATMAP":"",t.clearcoatRoughnessMap?"#define USE_CLEARCOAT_ROUGHNESSMAP":"",t.clearcoatNormalMap?"#define USE_CLEARCOAT_NORMALMAP":"",t.dispersion?"#define USE_DISPERSION":"",t.iridescence?"#define USE_IRIDESCENCE":"",t.iridescenceMap?"#define USE_IRIDESCENCEMAP":"",t.iridescenceThicknessMap?"#define USE_IRIDESCENCE_THICKNESSMAP":"",t.specularMap?"#define USE_SPECULARMAP":"",t.specularColorMap?"#define USE_SPECULAR_COLORMAP":"",t.specularIntensityMap?"#define USE_SPECULAR_INTENSITYMAP":"",t.roughnessMap?"#define USE_ROUGHNESSMAP":"",t.metalnessMap?"#define USE_METALNESSMAP":"",t.alphaMap?"#define USE_ALPHAMAP":"",t.alphaTest?"#define USE_ALPHATEST":"",t.alphaHash?"#define USE_ALPHAHASH":"",t.sheen?"#define USE_SHEEN":"",t.sheenColorMap?"#define USE_SHEEN_COLORMAP":"",t.sheenRoughnessMap?"#define USE_SHEEN_ROUGHNESSMAP":"",t.transmission?"#define USE_TRANSMISSION":"",t.transmissionMap?"#define USE_TRANSMISSIONMAP":"",t.thicknessMap?"#define USE_THICKNESSMAP":"",t.vertexTangents&&t.flatShading===!1?"#define USE_TANGENT":"",t.vertexColors||t.instancingColor?"#define USE_COLOR":"",t.vertexAlphas||t.batchingColor?"#define USE_COLOR_ALPHA":"",t.vertexUv1s?"#define USE_UV1":"",t.vertexUv2s?"#define USE_UV2":"",t.vertexUv3s?"#define USE_UV3":"",t.pointsUvs?"#define USE_POINTS_UV":"",t.gradientMap?"#define USE_GRADIENTMAP":"",t.flatShading?"#define FLAT_SHADED":"",t.doubleSided?"#define DOUBLE_SIDED":"",t.flipSided?"#define FLIP_SIDED":"",t.shadowMapEnabled?"#define USE_SHADOWMAP":"",t.shadowMapEnabled?"#define "+c:"",t.premultipliedAlpha?"#define PREMULTIPLIED_ALPHA":"",t.numLightProbes>0?"#define USE_LIGHT_PROBES":"",t.decodeVideoTexture?"#define DECODE_VIDEO_TEXTURE":"",t.decodeVideoTextureEmissive?"#define DECODE_VIDEO_TEXTURE_EMISSIVE":"",t.logarithmicDepthBuffer?"#define USE_LOGARITHMIC_DEPTH_BUFFER":"",t.reversedDepthBuffer?"#define USE_REVERSED_DEPTH_BUFFER":"","uniform mat4 viewMatrix;","uniform vec3 cameraPosition;","uniform bool isOrthographic;",t.toneMapping!==pi?"#define TONE_MAPPING":"",t.toneMapping!==pi?Ge.tonemapping_pars_fragment:"",t.toneMapping!==pi?eR("toneMapping",t.toneMapping):"",t.dithering?"#define DITHERING":"",t.opaque?"#define OPAQUE":"",Ge.colorspace_pars_fragment,K1("linearToOutputTexel",t.outputColorSpace),tR(),t.useDepthPacking?"#define DEPTH_PACKING "+t.depthPacking:"",`
`].filter(pc).join(`
`)),o=Ag(o),o=mM(o,t),o=gM(o,t),a=Ag(a),a=mM(a,t),a=gM(a,t),o=vM(o),a=vM(a),t.isRawShaderMaterial!==!0&&(M=`#version 300 es
`,m=[h,"#define attribute in","#define varying out","#define texture2D texture"].join(`
`)+`
`+m,p=["#define varying in",t.glslVersion===pg?"":"layout(location = 0) out highp vec4 pc_fragColor;",t.glslVersion===pg?"":"#define gl_FragColor pc_fragColor","#define gl_FragDepthEXT gl_FragDepth","#define texture2D texture","#define textureCube texture","#define texture2DProj textureProj","#define texture2DLodEXT textureLod","#define texture2DProjLodEXT textureProjLod","#define textureCubeLodEXT textureLod","#define texture2DGradEXT textureGrad","#define texture2DProjGradEXT textureProjGrad","#define textureCubeGradEXT textureGrad"].join(`
`)+`
`+p);let T=M+m+o,S=M+p+a,I=fM(r,r.VERTEX_SHADER,T),D=fM(r,r.FRAGMENT_SHADER,S);r.attachShader(x,I),r.attachShader(x,D),t.index0AttributeName!==void 0?r.bindAttribLocation(x,0,t.index0AttributeName):t.morphTargets===!0&&r.bindAttribLocation(x,0,"position"),r.linkProgram(x);function A(C){if(n.debug.checkShaderErrors){let U=r.getProgramInfoLog(x)||"",V=r.getShaderInfoLog(I)||"",W=r.getShaderInfoLog(D)||"",B=U.trim(),H=V.trim(),L=W.trim(),Q=!0,Z=!0;if(r.getProgramParameter(x,r.LINK_STATUS)===!1)if(Q=!1,typeof n.debug.onShaderError=="function")n.debug.onShaderError(r,x,I,D);else{let ue=pM(r,I,"vertex"),ge=pM(r,D,"fragment");Ne("THREE.WebGLProgram: Shader Error "+r.getError()+" - VALIDATE_STATUS "+r.getProgramParameter(x,r.VALIDATE_STATUS)+`

Material Name: `+C.name+`
Material Type: `+C.type+`

Program Info Log: `+B+`
`+ue+`
`+ge)}else B!==""?Le("WebGLProgram: Program Info Log:",B):(H===""||L==="")&&(Z=!1);Z&&(C.diagnostics={runnable:Q,programLog:B,vertexShader:{log:H,prefix:m},fragmentShader:{log:L,prefix:p}})}r.deleteShader(I),r.deleteShader(D),y=new Fo(r,x),E=rR(r,x)}let y;this.getUniforms=function(){return y===void 0&&A(this),y};let E;this.getAttributes=function(){return E===void 0&&A(this),E};let q=t.rendererExtensionParallelShaderCompile===!1;return this.isReady=function(){return q===!1&&(q=r.getProgramParameter(x,X1)),q},this.destroy=function(){i.releaseStatesOfProgram(this),r.deleteProgram(x),this.program=void 0},this.type=t.shaderType,this.name=t.shaderName,this.id=Y1++,this.cacheKey=e,this.usedTimes=1,this.program=x,this.vertexShader=I,this.fragmentShader=D,this}var xR=0,Rg=class{constructor(){this.shaderCache=new Map,this.materialCache=new Map}update(e){let t=e.vertexShader,i=e.fragmentShader,r=this._getShaderStage(t),s=this._getShaderStage(i),o=this._getShaderCacheForMaterial(e);return o.has(r)===!1&&(o.add(r),r.usedTimes++),o.has(s)===!1&&(o.add(s),s.usedTimes++),this}remove(e){let t=this.materialCache.get(e);for(let i of t)i.usedTimes--,i.usedTimes===0&&this.shaderCache.delete(i.code);return this.materialCache.delete(e),this}getVertexShaderID(e){return this._getShaderStage(e.vertexShader).id}getFragmentShaderID(e){return this._getShaderStage(e.fragmentShader).id}dispose(){this.shaderCache.clear(),this.materialCache.clear()}_getShaderCacheForMaterial(e){let t=this.materialCache,i=t.get(e);return i===void 0&&(i=new Set,t.set(e,i)),i}_getShaderStage(e){let t=this.shaderCache,i=t.get(e);return i===void 0&&(i=new Ng(e),t.set(e,i)),i}},Ng=class{constructor(e){this.id=xR++,this.code=e,this.usedTimes=0}};function MR(n,e,t,i,r,s){let o=new Ga,a=new Rg,c=new Set,l=[],u=new Map,f=i.logarithmicDepthBuffer,d=i.precision,h={MeshDepthMaterial:"depth",MeshDistanceMaterial:"distance",MeshNormalMaterial:"normal",MeshBasicMaterial:"basic",MeshLambertMaterial:"lambert",MeshPhongMaterial:"phong",MeshToonMaterial:"toon",MeshStandardMaterial:"physical",MeshPhysicalMaterial:"physical",MeshMatcapMaterial:"matcap",LineBasicMaterial:"basic",LineDashedMaterial:"dashed",PointsMaterial:"points",ShadowMaterial:"shadow",SpriteMaterial:"sprite"};function g(y){return c.add(y),y===0?"uv":`uv${y}`}function x(y,E,q,C,U){let V=C.fog,W=U.geometry,B=y.isMeshStandardMaterial||y.isMeshLambertMaterial||y.isMeshPhongMaterial?C.environment:null,H=y.isMeshStandardMaterial||y.isMeshLambertMaterial&&!y.envMap||y.isMeshPhongMaterial&&!y.envMap,L=e.get(y.envMap||B,H),Q=L&&L.mapping===sc?L.image.height:null,Z=h[y.type];y.precision!==null&&(d=i.getMaxPrecision(y.precision),d!==y.precision&&Le("WebGLProgram.getParameters:",y.precision,"not supported, using",d,"instead."));let ue=W.morphAttributes.position||W.morphAttributes.normal||W.morphAttributes.color,ge=ue!==void 0?ue.length:0,fe=0;W.morphAttributes.position!==void 0&&(fe=1),W.morphAttributes.normal!==void 0&&(fe=2),W.morphAttributes.color!==void 0&&(fe=3);let ze,Tt,St,X;if(Z){let ht=Li[Z];ze=ht.vertexShader,Tt=ht.fragmentShader}else ze=y.vertexShader,Tt=y.fragmentShader,a.update(y),St=a.getVertexShaderID(y),X=a.getFragmentShaderID(y);let ne=n.getRenderTarget(),se=n.state.buffers.depth.getReversed(),Ve=U.isInstancedMesh===!0,Ie=U.isBatchedMesh===!0,Fe=!!y.map,Yt=!!y.matcap,st=!!L,ft=!!y.aoMap,_t=!!y.lightMap,We=!!y.bumpMap,Ft=!!y.normalMap,w=!!y.displacementMap,Vt=!!y.emissiveMap,lt=!!y.metalnessMap,bt=!!y.roughnessMap,be=y.anisotropy>0,b=y.clearcoat>0,v=y.dispersion>0,N=y.iridescence>0,$=y.sheen>0,Y=y.transmission>0,j=be&&!!y.anisotropyMap,ve=b&&!!y.clearcoatMap,ie=b&&!!y.clearcoatNormalMap,Ce=b&&!!y.clearcoatRoughnessMap,Re=N&&!!y.iridescenceMap,J=N&&!!y.iridescenceThicknessMap,ee=$&&!!y.sheenColorMap,ye=$&&!!y.sheenRoughnessMap,xe=!!y.specularMap,de=!!y.specularColorMap,je=!!y.specularIntensityMap,R=Y&&!!y.transmissionMap,re=Y&&!!y.thicknessMap,te=!!y.gradientMap,me=!!y.alphaMap,K=y.alphaTest>0,G=!!y.alphaHash,_e=!!y.extensions,ke=pi;y.toneMapped&&(ne===null||ne.isXRRenderTarget===!0)&&(ke=n.toneMapping);let Et={shaderID:Z,shaderType:y.type,shaderName:y.name,vertexShader:ze,fragmentShader:Tt,defines:y.defines,customVertexShaderID:St,customFragmentShaderID:X,isRawShaderMaterial:y.isRawShaderMaterial===!0,glslVersion:y.glslVersion,precision:d,batching:Ie,batchingColor:Ie&&U._colorsTexture!==null,instancing:Ve,instancingColor:Ve&&U.instanceColor!==null,instancingMorph:Ve&&U.morphTexture!==null,outputColorSpace:ne===null?n.outputColorSpace:ne.isXRRenderTarget===!0?ne.texture.colorSpace:Es,alphaToCoverage:!!y.alphaToCoverage,map:Fe,matcap:Yt,envMap:st,envMapMode:st&&L.mapping,envMapCubeUVHeight:Q,aoMap:ft,lightMap:_t,bumpMap:We,normalMap:Ft,displacementMap:w,emissiveMap:Vt,normalMapObjectSpace:Ft&&y.normalMapType===zx,normalMapTangentSpace:Ft&&y.normalMapType===Hx,metalnessMap:lt,roughnessMap:bt,anisotropy:be,anisotropyMap:j,clearcoat:b,clearcoatMap:ve,clearcoatNormalMap:ie,clearcoatRoughnessMap:Ce,dispersion:v,iridescence:N,iridescenceMap:Re,iridescenceThicknessMap:J,sheen:$,sheenColorMap:ee,sheenRoughnessMap:ye,specularMap:xe,specularColorMap:de,specularIntensityMap:je,transmission:Y,transmissionMap:R,thicknessMap:re,gradientMap:te,opaque:y.transparent===!1&&y.blending===rr&&y.alphaToCoverage===!1,alphaMap:me,alphaTest:K,alphaHash:G,combine:y.combine,mapUv:Fe&&g(y.map.channel),aoMapUv:ft&&g(y.aoMap.channel),lightMapUv:_t&&g(y.lightMap.channel),bumpMapUv:We&&g(y.bumpMap.channel),normalMapUv:Ft&&g(y.normalMap.channel),displacementMapUv:w&&g(y.displacementMap.channel),emissiveMapUv:Vt&&g(y.emissiveMap.channel),metalnessMapUv:lt&&g(y.metalnessMap.channel),roughnessMapUv:bt&&g(y.roughnessMap.channel),anisotropyMapUv:j&&g(y.anisotropyMap.channel),clearcoatMapUv:ve&&g(y.clearcoatMap.channel),clearcoatNormalMapUv:ie&&g(y.clearcoatNormalMap.channel),clearcoatRoughnessMapUv:Ce&&g(y.clearcoatRoughnessMap.channel),iridescenceMapUv:Re&&g(y.iridescenceMap.channel),iridescenceThicknessMapUv:J&&g(y.iridescenceThicknessMap.channel),sheenColorMapUv:ee&&g(y.sheenColorMap.channel),sheenRoughnessMapUv:ye&&g(y.sheenRoughnessMap.channel),specularMapUv:xe&&g(y.specularMap.channel),specularColorMapUv:de&&g(y.specularColorMap.channel),specularIntensityMapUv:je&&g(y.specularIntensityMap.channel),transmissionMapUv:R&&g(y.transmissionMap.channel),thicknessMapUv:re&&g(y.thicknessMap.channel),alphaMapUv:me&&g(y.alphaMap.channel),vertexTangents:!!W.attributes.tangent&&(Ft||be),vertexColors:y.vertexColors,vertexAlphas:y.vertexColors===!0&&!!W.attributes.color&&W.attributes.color.itemSize===4,pointsUvs:U.isPoints===!0&&!!W.attributes.uv&&(Fe||me),fog:!!V,useFog:y.fog===!0,fogExp2:!!V&&V.isFogExp2,flatShading:y.wireframe===!1&&(y.flatShading===!0||W.attributes.normal===void 0&&Ft===!1&&(y.isMeshLambertMaterial||y.isMeshPhongMaterial||y.isMeshStandardMaterial||y.isMeshPhysicalMaterial)),sizeAttenuation:y.sizeAttenuation===!0,logarithmicDepthBuffer:f,reversedDepthBuffer:se,skinning:U.isSkinnedMesh===!0,morphTargets:W.morphAttributes.position!==void 0,morphNormals:W.morphAttributes.normal!==void 0,morphColors:W.morphAttributes.color!==void 0,morphTargetsCount:ge,morphTextureStride:fe,numDirLights:E.directional.length,numPointLights:E.point.length,numSpotLights:E.spot.length,numSpotLightMaps:E.spotLightMap.length,numRectAreaLights:E.rectArea.length,numHemiLights:E.hemi.length,numDirLightShadows:E.directionalShadowMap.length,numPointLightShadows:E.pointShadowMap.length,numSpotLightShadows:E.spotShadowMap.length,numSpotLightShadowsWithMaps:E.numSpotLightShadowsWithMaps,numLightProbes:E.numLightProbes,numClippingPlanes:s.numPlanes,numClipIntersection:s.numIntersection,dithering:y.dithering,shadowMapEnabled:n.shadowMap.enabled&&q.length>0,shadowMapType:n.shadowMap.type,toneMapping:ke,decodeVideoTexture:Fe&&y.map.isVideoTexture===!0&&rt.getTransfer(y.map.colorSpace)===dt,decodeVideoTextureEmissive:Vt&&y.emissiveMap.isVideoTexture===!0&&rt.getTransfer(y.emissiveMap.colorSpace)===dt,premultipliedAlpha:y.premultipliedAlpha,doubleSided:y.side===Ai,flipSided:y.side===xn,useDepthPacking:y.depthPacking>=0,depthPacking:y.depthPacking||0,index0AttributeName:y.index0AttributeName,extensionClipCullDistance:_e&&y.extensions.clipCullDistance===!0&&t.has("WEBGL_clip_cull_distance"),extensionMultiDraw:(_e&&y.extensions.multiDraw===!0||Ie)&&t.has("WEBGL_multi_draw"),rendererExtensionParallelShaderCompile:t.has("KHR_parallel_shader_compile"),customProgramCacheKey:y.customProgramCacheKey()};return Et.vertexUv1s=c.has(1),Et.vertexUv2s=c.has(2),Et.vertexUv3s=c.has(3),c.clear(),Et}function m(y){let E=[];if(y.shaderID?E.push(y.shaderID):(E.push(y.customVertexShaderID),E.push(y.customFragmentShaderID)),y.defines!==void 0)for(let q in y.defines)E.push(q),E.push(y.defines[q]);return y.isRawShaderMaterial===!1&&(p(E,y),M(E,y),E.push(n.outputColorSpace)),E.push(y.customProgramCacheKey),E.join()}function p(y,E){y.push(E.precision),y.push(E.outputColorSpace),y.push(E.envMapMode),y.push(E.envMapCubeUVHeight),y.push(E.mapUv),y.push(E.alphaMapUv),y.push(E.lightMapUv),y.push(E.aoMapUv),y.push(E.bumpMapUv),y.push(E.normalMapUv),y.push(E.displacementMapUv),y.push(E.emissiveMapUv),y.push(E.metalnessMapUv),y.push(E.roughnessMapUv),y.push(E.anisotropyMapUv),y.push(E.clearcoatMapUv),y.push(E.clearcoatNormalMapUv),y.push(E.clearcoatRoughnessMapUv),y.push(E.iridescenceMapUv),y.push(E.iridescenceThicknessMapUv),y.push(E.sheenColorMapUv),y.push(E.sheenRoughnessMapUv),y.push(E.specularMapUv),y.push(E.specularColorMapUv),y.push(E.specularIntensityMapUv),y.push(E.transmissionMapUv),y.push(E.thicknessMapUv),y.push(E.combine),y.push(E.fogExp2),y.push(E.sizeAttenuation),y.push(E.morphTargetsCount),y.push(E.morphAttributeCount),y.push(E.numDirLights),y.push(E.numPointLights),y.push(E.numSpotLights),y.push(E.numSpotLightMaps),y.push(E.numHemiLights),y.push(E.numRectAreaLights),y.push(E.numDirLightShadows),y.push(E.numPointLightShadows),y.push(E.numSpotLightShadows),y.push(E.numSpotLightShadowsWithMaps),y.push(E.numLightProbes),y.push(E.shadowMapType),y.push(E.toneMapping),y.push(E.numClippingPlanes),y.push(E.numClipIntersection),y.push(E.depthPacking)}function M(y,E){o.disableAll(),E.instancing&&o.enable(0),E.instancingColor&&o.enable(1),E.instancingMorph&&o.enable(2),E.matcap&&o.enable(3),E.envMap&&o.enable(4),E.normalMapObjectSpace&&o.enable(5),E.normalMapTangentSpace&&o.enable(6),E.clearcoat&&o.enable(7),E.iridescence&&o.enable(8),E.alphaTest&&o.enable(9),E.vertexColors&&o.enable(10),E.vertexAlphas&&o.enable(11),E.vertexUv1s&&o.enable(12),E.vertexUv2s&&o.enable(13),E.vertexUv3s&&o.enable(14),E.vertexTangents&&o.enable(15),E.anisotropy&&o.enable(16),E.alphaHash&&o.enable(17),E.batching&&o.enable(18),E.dispersion&&o.enable(19),E.batchingColor&&o.enable(20),E.gradientMap&&o.enable(21),y.push(o.mask),o.disableAll(),E.fog&&o.enable(0),E.useFog&&o.enable(1),E.flatShading&&o.enable(2),E.logarithmicDepthBuffer&&o.enable(3),E.reversedDepthBuffer&&o.enable(4),E.skinning&&o.enable(5),E.morphTargets&&o.enable(6),E.morphNormals&&o.enable(7),E.morphColors&&o.enable(8),E.premultipliedAlpha&&o.enable(9),E.shadowMapEnabled&&o.enable(10),E.doubleSided&&o.enable(11),E.flipSided&&o.enable(12),E.useDepthPacking&&o.enable(13),E.dithering&&o.enable(14),E.transmission&&o.enable(15),E.sheen&&o.enable(16),E.opaque&&o.enable(17),E.pointsUvs&&o.enable(18),E.decodeVideoTexture&&o.enable(19),E.decodeVideoTextureEmissive&&o.enable(20),E.alphaToCoverage&&o.enable(21),y.push(o.mask)}function T(y){let E=h[y.type],q;if(E){let C=Li[E];q=eM.clone(C.uniforms)}else q=y.uniforms;return q}function S(y,E){let q=u.get(E);return q!==void 0?++q.usedTimes:(q=new _R(n,E,y,r),l.push(q),u.set(E,q)),q}function I(y){if(--y.usedTimes===0){let E=l.indexOf(y);l[E]=l[l.length-1],l.pop(),u.delete(y.cacheKey),y.destroy()}}function D(y){a.remove(y)}function A(){a.dispose()}return{getParameters:x,getProgramCacheKey:m,getUniforms:T,acquireProgram:S,releaseProgram:I,releaseShaderCache:D,programs:l,dispose:A}}function bR(){let n=new WeakMap;function e(o){return n.has(o)}function t(o){let a=n.get(o);return a===void 0&&(a={},n.set(o,a)),a}function i(o){n.delete(o)}function r(o,a,c){n.get(o)[a]=c}function s(){n=new WeakMap}return{has:e,get:t,remove:i,update:r,dispose:s}}function ER(n,e){return n.groupOrder!==e.groupOrder?n.groupOrder-e.groupOrder:n.renderOrder!==e.renderOrder?n.renderOrder-e.renderOrder:n.material.id!==e.material.id?n.material.id-e.material.id:n.materialVariant!==e.materialVariant?n.materialVariant-e.materialVariant:n.z!==e.z?n.z-e.z:n.id-e.id}function _M(n,e){return n.groupOrder!==e.groupOrder?n.groupOrder-e.groupOrder:n.renderOrder!==e.renderOrder?n.renderOrder-e.renderOrder:n.z!==e.z?e.z-n.z:n.id-e.id}function xM(){let n=[],e=0,t=[],i=[],r=[];function s(){e=0,t.length=0,i.length=0,r.length=0}function o(d){let h=0;return d.isInstancedMesh&&(h+=2),d.isSkinnedMesh&&(h+=1),h}function a(d,h,g,x,m,p){let M=n[e];return M===void 0?(M={id:d.id,object:d,geometry:h,material:g,materialVariant:o(d),groupOrder:x,renderOrder:d.renderOrder,z:m,group:p},n[e]=M):(M.id=d.id,M.object=d,M.geometry=h,M.material=g,M.materialVariant=o(d),M.groupOrder=x,M.renderOrder=d.renderOrder,M.z=m,M.group=p),e++,M}function c(d,h,g,x,m,p){let M=a(d,h,g,x,m,p);g.transmission>0?i.push(M):g.transparent===!0?r.push(M):t.push(M)}function l(d,h,g,x,m,p){let M=a(d,h,g,x,m,p);g.transmission>0?i.unshift(M):g.transparent===!0?r.unshift(M):t.unshift(M)}function u(d,h){t.length>1&&t.sort(d||ER),i.length>1&&i.sort(h||_M),r.length>1&&r.sort(h||_M)}function f(){for(let d=e,h=n.length;d<h;d++){let g=n[d];if(g.id===null)break;g.id=null,g.object=null,g.geometry=null,g.material=null,g.group=null}}return{opaque:t,transmissive:i,transparent:r,init:s,push:c,unshift:l,finish:f,sort:u}}function SR(){let n=new WeakMap;function e(i,r){let s=n.get(i),o;return s===void 0?(o=new xM,n.set(i,[o])):r>=s.length?(o=new xM,s.push(o)):o=s[r],o}function t(){n=new WeakMap}return{get:e,dispose:t}}function TR(){let n={};return{get:function(e){if(n[e.id]!==void 0)return n[e.id];let t;switch(e.type){case"DirectionalLight":t={direction:new k,color:new Xe};break;case"SpotLight":t={position:new k,direction:new k,color:new Xe,distance:0,coneCos:0,penumbraCos:0,decay:0};break;case"PointLight":t={position:new k,color:new Xe,distance:0,decay:0};break;case"HemisphereLight":t={direction:new k,skyColor:new Xe,groundColor:new Xe};break;case"RectAreaLight":t={color:new Xe,position:new k,halfWidth:new k,halfHeight:new k};break}return n[e.id]=t,t}}}function CR(){let n={};return{get:function(e){if(n[e.id]!==void 0)return n[e.id];let t;switch(e.type){case"DirectionalLight":t={shadowIntensity:1,shadowBias:0,shadowNormalBias:0,shadowRadius:1,shadowMapSize:new pt};break;case"SpotLight":t={shadowIntensity:1,shadowBias:0,shadowNormalBias:0,shadowRadius:1,shadowMapSize:new pt};break;case"PointLight":t={shadowIntensity:1,shadowBias:0,shadowNormalBias:0,shadowRadius:1,shadowMapSize:new pt,shadowCameraNear:1,shadowCameraFar:1e3};break}return n[e.id]=t,t}}}var wR=0;function DR(n,e){return(e.castShadow?2:0)-(n.castShadow?2:0)+(e.map?1:0)-(n.map?1:0)}function IR(n){let e=new TR,t=CR(),i={version:0,hash:{directionalLength:-1,pointLength:-1,spotLength:-1,rectAreaLength:-1,hemiLength:-1,numDirectionalShadows:-1,numPointShadows:-1,numSpotShadows:-1,numSpotMaps:-1,numLightProbes:-1},ambient:[0,0,0],probe:[],directional:[],directionalShadow:[],directionalShadowMap:[],directionalShadowMatrix:[],spot:[],spotLightMap:[],spotShadow:[],spotShadowMap:[],spotLightMatrix:[],rectArea:[],rectAreaLTC1:null,rectAreaLTC2:null,point:[],pointShadow:[],pointShadowMap:[],pointShadowMatrix:[],hemi:[],numSpotLightShadowsWithMaps:0,numLightProbes:0};for(let l=0;l<9;l++)i.probe.push(new k);let r=new k,s=new Bt,o=new Bt;function a(l){let u=0,f=0,d=0;for(let E=0;E<9;E++)i.probe[E].set(0,0,0);let h=0,g=0,x=0,m=0,p=0,M=0,T=0,S=0,I=0,D=0,A=0;l.sort(DR);for(let E=0,q=l.length;E<q;E++){let C=l[E],U=C.color,V=C.intensity,W=C.distance,B=null;if(C.shadow&&C.shadow.map&&(C.shadow.map.texture.format===ws?B=C.shadow.map.texture:B=C.shadow.map.depthTexture||C.shadow.map.texture),C.isAmbientLight)u+=U.r*V,f+=U.g*V,d+=U.b*V;else if(C.isLightProbe){for(let H=0;H<9;H++)i.probe[H].addScaledVector(C.sh.coefficients[H],V);A++}else if(C.isDirectionalLight){let H=e.get(C);if(H.color.copy(C.color).multiplyScalar(C.intensity),C.castShadow){let L=C.shadow,Q=t.get(C);Q.shadowIntensity=L.intensity,Q.shadowBias=L.bias,Q.shadowNormalBias=L.normalBias,Q.shadowRadius=L.radius,Q.shadowMapSize=L.mapSize,i.directionalShadow[h]=Q,i.directionalShadowMap[h]=B,i.directionalShadowMatrix[h]=C.shadow.matrix,M++}i.directional[h]=H,h++}else if(C.isSpotLight){let H=e.get(C);H.position.setFromMatrixPosition(C.matrixWorld),H.color.copy(U).multiplyScalar(V),H.distance=W,H.coneCos=Math.cos(C.angle),H.penumbraCos=Math.cos(C.angle*(1-C.penumbra)),H.decay=C.decay,i.spot[x]=H;let L=C.shadow;if(C.map&&(i.spotLightMap[I]=C.map,I++,L.updateMatrices(C),C.castShadow&&D++),i.spotLightMatrix[x]=L.matrix,C.castShadow){let Q=t.get(C);Q.shadowIntensity=L.intensity,Q.shadowBias=L.bias,Q.shadowNormalBias=L.normalBias,Q.shadowRadius=L.radius,Q.shadowMapSize=L.mapSize,i.spotShadow[x]=Q,i.spotShadowMap[x]=B,S++}x++}else if(C.isRectAreaLight){let H=e.get(C);H.color.copy(U).multiplyScalar(V),H.halfWidth.set(C.width*.5,0,0),H.halfHeight.set(0,C.height*.5,0),i.rectArea[m]=H,m++}else if(C.isPointLight){let H=e.get(C);if(H.color.copy(C.color).multiplyScalar(C.intensity),H.distance=C.distance,H.decay=C.decay,C.castShadow){let L=C.shadow,Q=t.get(C);Q.shadowIntensity=L.intensity,Q.shadowBias=L.bias,Q.shadowNormalBias=L.normalBias,Q.shadowRadius=L.radius,Q.shadowMapSize=L.mapSize,Q.shadowCameraNear=L.camera.near,Q.shadowCameraFar=L.camera.far,i.pointShadow[g]=Q,i.pointShadowMap[g]=B,i.pointShadowMatrix[g]=C.shadow.matrix,T++}i.point[g]=H,g++}else if(C.isHemisphereLight){let H=e.get(C);H.skyColor.copy(C.color).multiplyScalar(V),H.groundColor.copy(C.groundColor).multiplyScalar(V),i.hemi[p]=H,p++}}m>0&&(n.has("OES_texture_float_linear")===!0?(i.rectAreaLTC1=ae.LTC_FLOAT_1,i.rectAreaLTC2=ae.LTC_FLOAT_2):(i.rectAreaLTC1=ae.LTC_HALF_1,i.rectAreaLTC2=ae.LTC_HALF_2)),i.ambient[0]=u,i.ambient[1]=f,i.ambient[2]=d;let y=i.hash;(y.directionalLength!==h||y.pointLength!==g||y.spotLength!==x||y.rectAreaLength!==m||y.hemiLength!==p||y.numDirectionalShadows!==M||y.numPointShadows!==T||y.numSpotShadows!==S||y.numSpotMaps!==I||y.numLightProbes!==A)&&(i.directional.length=h,i.spot.length=x,i.rectArea.length=m,i.point.length=g,i.hemi.length=p,i.directionalShadow.length=M,i.directionalShadowMap.length=M,i.pointShadow.length=T,i.pointShadowMap.length=T,i.spotShadow.length=S,i.spotShadowMap.length=S,i.directionalShadowMatrix.length=M,i.pointShadowMatrix.length=T,i.spotLightMatrix.length=S+I-D,i.spotLightMap.length=I,i.numSpotLightShadowsWithMaps=D,i.numLightProbes=A,y.directionalLength=h,y.pointLength=g,y.spotLength=x,y.rectAreaLength=m,y.hemiLength=p,y.numDirectionalShadows=M,y.numPointShadows=T,y.numSpotShadows=S,y.numSpotMaps=I,y.numLightProbes=A,i.version=wR++)}function c(l,u){let f=0,d=0,h=0,g=0,x=0,m=u.matrixWorldInverse;for(let p=0,M=l.length;p<M;p++){let T=l[p];if(T.isDirectionalLight){let S=i.directional[f];S.direction.setFromMatrixPosition(T.matrixWorld),r.setFromMatrixPosition(T.target.matrixWorld),S.direction.sub(r),S.direction.transformDirection(m),f++}else if(T.isSpotLight){let S=i.spot[h];S.position.setFromMatrixPosition(T.matrixWorld),S.position.applyMatrix4(m),S.direction.setFromMatrixPosition(T.matrixWorld),r.setFromMatrixPosition(T.target.matrixWorld),S.direction.sub(r),S.direction.transformDirection(m),h++}else if(T.isRectAreaLight){let S=i.rectArea[g];S.position.setFromMatrixPosition(T.matrixWorld),S.position.applyMatrix4(m),o.identity(),s.copy(T.matrixWorld),s.premultiply(m),o.extractRotation(s),S.halfWidth.set(T.width*.5,0,0),S.halfHeight.set(0,T.height*.5,0),S.halfWidth.applyMatrix4(o),S.halfHeight.applyMatrix4(o),g++}else if(T.isPointLight){let S=i.point[d];S.position.setFromMatrixPosition(T.matrixWorld),S.position.applyMatrix4(m),d++}else if(T.isHemisphereLight){let S=i.hemi[x];S.direction.setFromMatrixPosition(T.matrixWorld),S.direction.transformDirection(m),x++}}}return{setup:a,setupView:c,state:i}}function MM(n){let e=new IR(n),t=[],i=[];function r(u){l.camera=u,t.length=0,i.length=0}function s(u){t.push(u)}function o(u){i.push(u)}function a(){e.setup(t)}function c(u){e.setupView(t,u)}let l={lightsArray:t,shadowsArray:i,camera:null,lights:e,transmissionRenderTarget:{}};return{init:r,state:l,setupLights:a,setupLightsView:c,pushLight:s,pushShadow:o}}function AR(n){let e=new WeakMap;function t(r,s=0){let o=e.get(r),a;return o===void 0?(a=new MM(n),e.set(r,[a])):s>=o.length?(a=new MM(n),o.push(a)):a=o[s],a}function i(){e=new WeakMap}return{get:t,dispose:i}}var RR=`void main() {
	gl_Position = vec4( position, 1.0 );
}`,NR=`uniform sampler2D shadow_pass;
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
}`,PR=[new k(1,0,0),new k(-1,0,0),new k(0,1,0),new k(0,-1,0),new k(0,0,1),new k(0,0,-1)],LR=[new k(0,-1,0),new k(0,-1,0),new k(0,0,1),new k(0,0,-1),new k(0,-1,0),new k(0,-1,0)],bM=new Bt,hc=new k,Tg=new k;function FR(n,e,t){let i=new Ya,r=new pt,s=new pt,o=new Lt,a=new $u,c=new qu,l={},u=t.maxTextureSize,f={[ir]:xn,[xn]:ir,[Ai]:Ai},d=new On({defines:{VSM_SAMPLES:8},uniforms:{shadow_pass:{value:null},resolution:{value:new pt},radius:{value:4}},vertexShader:RR,fragmentShader:NR}),h=d.clone();h.defines.HORIZONTAL_PASS=1;let g=new Fn;g.setAttribute("position",new pn(new Float32Array([-1,-1,.5,3,-1,.5,-1,3,.5]),3));let x=new Zn(g,d),m=this;this.enabled=!1,this.autoUpdate=!0,this.needsUpdate=!1,this.type=rc;let p=this.type;this.render=function(D,A,y){if(m.enabled===!1||m.autoUpdate===!1&&m.needsUpdate===!1||D.length===0)return;this.type===yx&&(Le("WebGLShadowMap: PCFSoftShadowMap has been deprecated. Using PCFShadowMap instead."),this.type=rc);let E=n.getRenderTarget(),q=n.getActiveCubeFace(),C=n.getActiveMipmapLevel(),U=n.state;U.setBlending(Ri),U.buffers.depth.getReversed()===!0?U.buffers.color.setClear(0,0,0,0):U.buffers.color.setClear(1,1,1,1),U.buffers.depth.setTest(!0),U.setScissorTest(!1);let V=p!==this.type;V&&A.traverse(function(W){W.material&&(Array.isArray(W.material)?W.material.forEach(B=>B.needsUpdate=!0):W.material.needsUpdate=!0)});for(let W=0,B=D.length;W<B;W++){let H=D[W],L=H.shadow;if(L===void 0){Le("WebGLShadowMap:",H,"has no shadow.");continue}if(L.autoUpdate===!1&&L.needsUpdate===!1)continue;r.copy(L.mapSize);let Q=L.getFrameExtents();r.multiply(Q),s.copy(L.mapSize),(r.x>u||r.y>u)&&(r.x>u&&(s.x=Math.floor(u/Q.x),r.x=s.x*Q.x,L.mapSize.x=s.x),r.y>u&&(s.y=Math.floor(u/Q.y),r.y=s.y*Q.y,L.mapSize.y=s.y));let Z=n.state.buffers.depth.getReversed();if(L.camera._reversedDepth=Z,L.map===null||V===!0){if(L.map!==null&&(L.map.depthTexture!==null&&(L.map.depthTexture.dispose(),L.map.depthTexture=null),L.map.dispose()),this.type===Ro){if(H.isPointLight){Le("WebGLShadowMap: VSM shadow maps are not supported for PointLights. Use PCF or BasicShadowMap instead.");continue}L.map=new Ln(r.x,r.y,{format:ws,type:Ni,minFilter:sn,magFilter:sn,generateMipmaps:!1}),L.map.texture.name=H.name+".shadowMap",L.map.depthTexture=new Fr(r.x,r.y,gi),L.map.depthTexture.name=H.name+".shadowMapDepth",L.map.depthTexture.format=wi,L.map.depthTexture.compareFunction=null,L.map.depthTexture.minFilter=en,L.map.depthTexture.magFilter=en}else H.isPointLight?(L.map=new Jd(r.x),L.map.depthTexture=new Wu(r.x,mi)):(L.map=new Ln(r.x,r.y),L.map.depthTexture=new Fr(r.x,r.y,mi)),L.map.depthTexture.name=H.name+".shadowMap",L.map.depthTexture.format=wi,this.type===rc?(L.map.depthTexture.compareFunction=Z?qd:$d,L.map.depthTexture.minFilter=sn,L.map.depthTexture.magFilter=sn):(L.map.depthTexture.compareFunction=null,L.map.depthTexture.minFilter=en,L.map.depthTexture.magFilter=en);L.camera.updateProjectionMatrix()}let ue=L.map.isWebGLCubeRenderTarget?6:1;for(let ge=0;ge<ue;ge++){if(L.map.isWebGLCubeRenderTarget)n.setRenderTarget(L.map,ge),n.clear();else{ge===0&&(n.setRenderTarget(L.map),n.clear());let fe=L.getViewport(ge);o.set(s.x*fe.x,s.y*fe.y,s.x*fe.z,s.y*fe.w),U.viewport(o)}if(H.isPointLight){let fe=L.camera,ze=L.matrix,Tt=H.distance||fe.far;Tt!==fe.far&&(fe.far=Tt,fe.updateProjectionMatrix()),hc.setFromMatrixPosition(H.matrixWorld),fe.position.copy(hc),Tg.copy(fe.position),Tg.add(PR[ge]),fe.up.copy(LR[ge]),fe.lookAt(Tg),fe.updateMatrixWorld(),ze.makeTranslation(-hc.x,-hc.y,-hc.z),bM.multiplyMatrices(fe.projectionMatrix,fe.matrixWorldInverse),L._frustum.setFromProjectionMatrix(bM,fe.coordinateSystem,fe.reversedDepth)}else L.updateMatrices(H);i=L.getFrustum(),S(A,y,L.camera,H,this.type)}L.isPointLightShadow!==!0&&this.type===Ro&&M(L,y),L.needsUpdate=!1}p=this.type,m.needsUpdate=!1,n.setRenderTarget(E,q,C)};function M(D,A){let y=e.update(x);d.defines.VSM_SAMPLES!==D.blurSamples&&(d.defines.VSM_SAMPLES=D.blurSamples,h.defines.VSM_SAMPLES=D.blurSamples,d.needsUpdate=!0,h.needsUpdate=!0),D.mapPass===null&&(D.mapPass=new Ln(r.x,r.y,{format:ws,type:Ni})),d.uniforms.shadow_pass.value=D.map.depthTexture,d.uniforms.resolution.value=D.mapSize,d.uniforms.radius.value=D.radius,n.setRenderTarget(D.mapPass),n.clear(),n.renderBufferDirect(A,null,y,d,x,null),h.uniforms.shadow_pass.value=D.mapPass.texture,h.uniforms.resolution.value=D.mapSize,h.uniforms.radius.value=D.radius,n.setRenderTarget(D.map),n.clear(),n.renderBufferDirect(A,null,y,h,x,null)}function T(D,A,y,E){let q=null,C=y.isPointLight===!0?D.customDistanceMaterial:D.customDepthMaterial;if(C!==void 0)q=C;else if(q=y.isPointLight===!0?c:a,n.localClippingEnabled&&A.clipShadows===!0&&Array.isArray(A.clippingPlanes)&&A.clippingPlanes.length!==0||A.displacementMap&&A.displacementScale!==0||A.alphaMap&&A.alphaTest>0||A.map&&A.alphaTest>0||A.alphaToCoverage===!0){let U=q.uuid,V=A.uuid,W=l[U];W===void 0&&(W={},l[U]=W);let B=W[V];B===void 0&&(B=q.clone(),W[V]=B,A.addEventListener("dispose",I)),q=B}if(q.visible=A.visible,q.wireframe=A.wireframe,E===Ro?q.side=A.shadowSide!==null?A.shadowSide:A.side:q.side=A.shadowSide!==null?A.shadowSide:f[A.side],q.alphaMap=A.alphaMap,q.alphaTest=A.alphaToCoverage===!0?.5:A.alphaTest,q.map=A.map,q.clipShadows=A.clipShadows,q.clippingPlanes=A.clippingPlanes,q.clipIntersection=A.clipIntersection,q.displacementMap=A.displacementMap,q.displacementScale=A.displacementScale,q.displacementBias=A.displacementBias,q.wireframeLinewidth=A.wireframeLinewidth,q.linewidth=A.linewidth,y.isPointLight===!0&&q.isMeshDistanceMaterial===!0){let U=n.properties.get(q);U.light=y}return q}function S(D,A,y,E,q){if(D.visible===!1)return;if(D.layers.test(A.layers)&&(D.isMesh||D.isLine||D.isPoints)&&(D.castShadow||D.receiveShadow&&q===Ro)&&(!D.frustumCulled||i.intersectsObject(D))){D.modelViewMatrix.multiplyMatrices(y.matrixWorldInverse,D.matrixWorld);let V=e.update(D),W=D.material;if(Array.isArray(W)){let B=V.groups;for(let H=0,L=B.length;H<L;H++){let Q=B[H],Z=W[Q.materialIndex];if(Z&&Z.visible){let ue=T(D,Z,E,q);D.onBeforeShadow(n,D,A,y,V,ue,Q),n.renderBufferDirect(y,null,V,ue,D,Q),D.onAfterShadow(n,D,A,y,V,ue,Q)}}}else if(W.visible){let B=T(D,W,E,q);D.onBeforeShadow(n,D,A,y,V,B,null),n.renderBufferDirect(y,null,V,B,D,null),D.onAfterShadow(n,D,A,y,V,B,null)}}let U=D.children;for(let V=0,W=U.length;V<W;V++)S(U[V],A,y,E,q)}function I(D){D.target.removeEventListener("dispose",I);for(let y in l){let E=l[y],q=D.target.uuid;q in E&&(E[q].dispose(),delete E[q])}}}function OR(n,e){function t(){let R=!1,re=new Lt,te=null,me=new Lt(0,0,0,0);return{setMask:function(K){te!==K&&!R&&(n.colorMask(K,K,K,K),te=K)},setLocked:function(K){R=K},setClear:function(K,G,_e,ke,Et){Et===!0&&(K*=ke,G*=ke,_e*=ke),re.set(K,G,_e,ke),me.equals(re)===!1&&(n.clearColor(K,G,_e,ke),me.copy(re))},reset:function(){R=!1,te=null,me.set(-1,0,0,0)}}}function i(){let R=!1,re=!1,te=null,me=null,K=null;return{setReversed:function(G){if(re!==G){let _e=e.get("EXT_clip_control");G?_e.clipControlEXT(_e.LOWER_LEFT_EXT,_e.ZERO_TO_ONE_EXT):_e.clipControlEXT(_e.LOWER_LEFT_EXT,_e.NEGATIVE_ONE_TO_ONE_EXT),re=G;let ke=K;K=null,this.setClear(ke)}},getReversed:function(){return re},setTest:function(G){G?ne(n.DEPTH_TEST):se(n.DEPTH_TEST)},setMask:function(G){te!==G&&!R&&(n.depthMask(G),te=G)},setFunc:function(G){if(re&&(G=Kx[G]),me!==G){switch(G){case Iu:n.depthFunc(n.NEVER);break;case Au:n.depthFunc(n.ALWAYS);break;case Ru:n.depthFunc(n.LESS);break;case bs:n.depthFunc(n.LEQUAL);break;case Nu:n.depthFunc(n.EQUAL);break;case Pu:n.depthFunc(n.GEQUAL);break;case Lu:n.depthFunc(n.GREATER);break;case Fu:n.depthFunc(n.NOTEQUAL);break;default:n.depthFunc(n.LEQUAL)}me=G}},setLocked:function(G){R=G},setClear:function(G){K!==G&&(K=G,re&&(G=1-G),n.clearDepth(G))},reset:function(){R=!1,te=null,me=null,K=null,re=!1}}}function r(){let R=!1,re=null,te=null,me=null,K=null,G=null,_e=null,ke=null,Et=null;return{setTest:function(ht){R||(ht?ne(n.STENCIL_TEST):se(n.STENCIL_TEST))},setMask:function(ht){re!==ht&&!R&&(n.stencilMask(ht),re=ht)},setFunc:function(ht,Fi,Oi){(te!==ht||me!==Fi||K!==Oi)&&(n.stencilFunc(ht,Fi,Oi),te=ht,me=Fi,K=Oi)},setOp:function(ht,Fi,Oi){(G!==ht||_e!==Fi||ke!==Oi)&&(n.stencilOp(ht,Fi,Oi),G=ht,_e=Fi,ke=Oi)},setLocked:function(ht){R=ht},setClear:function(ht){Et!==ht&&(n.clearStencil(ht),Et=ht)},reset:function(){R=!1,re=null,te=null,me=null,K=null,G=null,_e=null,ke=null,Et=null}}}let s=new t,o=new i,a=new r,c=new WeakMap,l=new WeakMap,u={},f={},d=new WeakMap,h=[],g=null,x=!1,m=null,p=null,M=null,T=null,S=null,I=null,D=null,A=new Xe(0,0,0),y=0,E=!1,q=null,C=null,U=null,V=null,W=null,B=n.getParameter(n.MAX_COMBINED_TEXTURE_IMAGE_UNITS),H=!1,L=0,Q=n.getParameter(n.VERSION);Q.indexOf("WebGL")!==-1?(L=parseFloat(/^WebGL (\d)/.exec(Q)[1]),H=L>=1):Q.indexOf("OpenGL ES")!==-1&&(L=parseFloat(/^OpenGL ES (\d)/.exec(Q)[1]),H=L>=2);let Z=null,ue={},ge=n.getParameter(n.SCISSOR_BOX),fe=n.getParameter(n.VIEWPORT),ze=new Lt().fromArray(ge),Tt=new Lt().fromArray(fe);function St(R,re,te,me){let K=new Uint8Array(4),G=n.createTexture();n.bindTexture(R,G),n.texParameteri(R,n.TEXTURE_MIN_FILTER,n.NEAREST),n.texParameteri(R,n.TEXTURE_MAG_FILTER,n.NEAREST);for(let _e=0;_e<te;_e++)R===n.TEXTURE_3D||R===n.TEXTURE_2D_ARRAY?n.texImage3D(re,0,n.RGBA,1,1,me,0,n.RGBA,n.UNSIGNED_BYTE,K):n.texImage2D(re+_e,0,n.RGBA,1,1,0,n.RGBA,n.UNSIGNED_BYTE,K);return G}let X={};X[n.TEXTURE_2D]=St(n.TEXTURE_2D,n.TEXTURE_2D,1),X[n.TEXTURE_CUBE_MAP]=St(n.TEXTURE_CUBE_MAP,n.TEXTURE_CUBE_MAP_POSITIVE_X,6),X[n.TEXTURE_2D_ARRAY]=St(n.TEXTURE_2D_ARRAY,n.TEXTURE_2D_ARRAY,1,1),X[n.TEXTURE_3D]=St(n.TEXTURE_3D,n.TEXTURE_3D,1,1),s.setClear(0,0,0,1),o.setClear(1),a.setClear(0),ne(n.DEPTH_TEST),o.setFunc(bs),We(!1),Ft(Ym),ne(n.CULL_FACE),ft(Ri);function ne(R){u[R]!==!0&&(n.enable(R),u[R]=!0)}function se(R){u[R]!==!1&&(n.disable(R),u[R]=!1)}function Ve(R,re){return f[R]!==re?(n.bindFramebuffer(R,re),f[R]=re,R===n.DRAW_FRAMEBUFFER&&(f[n.FRAMEBUFFER]=re),R===n.FRAMEBUFFER&&(f[n.DRAW_FRAMEBUFFER]=re),!0):!1}function Ie(R,re){let te=h,me=!1;if(R){te=d.get(re),te===void 0&&(te=[],d.set(re,te));let K=R.textures;if(te.length!==K.length||te[0]!==n.COLOR_ATTACHMENT0){for(let G=0,_e=K.length;G<_e;G++)te[G]=n.COLOR_ATTACHMENT0+G;te.length=K.length,me=!0}}else te[0]!==n.BACK&&(te[0]=n.BACK,me=!0);me&&n.drawBuffers(te)}function Fe(R){return g!==R?(n.useProgram(R),g=R,!0):!1}let Yt={[Pr]:n.FUNC_ADD,[xx]:n.FUNC_SUBTRACT,[Mx]:n.FUNC_REVERSE_SUBTRACT};Yt[bx]=n.MIN,Yt[Ex]=n.MAX;let st={[Sx]:n.ZERO,[Tx]:n.ONE,[Cx]:n.SRC_COLOR,[wu]:n.SRC_ALPHA,[Nx]:n.SRC_ALPHA_SATURATE,[Ax]:n.DST_COLOR,[Dx]:n.DST_ALPHA,[wx]:n.ONE_MINUS_SRC_COLOR,[Du]:n.ONE_MINUS_SRC_ALPHA,[Rx]:n.ONE_MINUS_DST_COLOR,[Ix]:n.ONE_MINUS_DST_ALPHA,[Px]:n.CONSTANT_COLOR,[Lx]:n.ONE_MINUS_CONSTANT_COLOR,[Fx]:n.CONSTANT_ALPHA,[Ox]:n.ONE_MINUS_CONSTANT_ALPHA};function ft(R,re,te,me,K,G,_e,ke,Et,ht){if(R===Ri){x===!0&&(se(n.BLEND),x=!1);return}if(x===!1&&(ne(n.BLEND),x=!0),R!==_x){if(R!==m||ht!==E){if((p!==Pr||S!==Pr)&&(n.blendEquation(n.FUNC_ADD),p=Pr,S=Pr),ht)switch(R){case rr:n.blendFuncSeparate(n.ONE,n.ONE_MINUS_SRC_ALPHA,n.ONE,n.ONE_MINUS_SRC_ALPHA);break;case Zm:n.blendFunc(n.ONE,n.ONE);break;case Jm:n.blendFuncSeparate(n.ZERO,n.ONE_MINUS_SRC_COLOR,n.ZERO,n.ONE);break;case Km:n.blendFuncSeparate(n.DST_COLOR,n.ONE_MINUS_SRC_ALPHA,n.ZERO,n.ONE);break;default:Ne("WebGLState: Invalid blending: ",R);break}else switch(R){case rr:n.blendFuncSeparate(n.SRC_ALPHA,n.ONE_MINUS_SRC_ALPHA,n.ONE,n.ONE_MINUS_SRC_ALPHA);break;case Zm:n.blendFuncSeparate(n.SRC_ALPHA,n.ONE,n.ONE,n.ONE);break;case Jm:Ne("WebGLState: SubtractiveBlending requires material.premultipliedAlpha = true");break;case Km:Ne("WebGLState: MultiplyBlending requires material.premultipliedAlpha = true");break;default:Ne("WebGLState: Invalid blending: ",R);break}M=null,T=null,I=null,D=null,A.set(0,0,0),y=0,m=R,E=ht}return}K=K||re,G=G||te,_e=_e||me,(re!==p||K!==S)&&(n.blendEquationSeparate(Yt[re],Yt[K]),p=re,S=K),(te!==M||me!==T||G!==I||_e!==D)&&(n.blendFuncSeparate(st[te],st[me],st[G],st[_e]),M=te,T=me,I=G,D=_e),(ke.equals(A)===!1||Et!==y)&&(n.blendColor(ke.r,ke.g,ke.b,Et),A.copy(ke),y=Et),m=R,E=!1}function _t(R,re){R.side===Ai?se(n.CULL_FACE):ne(n.CULL_FACE);let te=R.side===xn;re&&(te=!te),We(te),R.blending===rr&&R.transparent===!1?ft(Ri):ft(R.blending,R.blendEquation,R.blendSrc,R.blendDst,R.blendEquationAlpha,R.blendSrcAlpha,R.blendDstAlpha,R.blendColor,R.blendAlpha,R.premultipliedAlpha),o.setFunc(R.depthFunc),o.setTest(R.depthTest),o.setMask(R.depthWrite),s.setMask(R.colorWrite);let me=R.stencilWrite;a.setTest(me),me&&(a.setMask(R.stencilWriteMask),a.setFunc(R.stencilFunc,R.stencilRef,R.stencilFuncMask),a.setOp(R.stencilFail,R.stencilZFail,R.stencilZPass)),Vt(R.polygonOffset,R.polygonOffsetFactor,R.polygonOffsetUnits),R.alphaToCoverage===!0?ne(n.SAMPLE_ALPHA_TO_COVERAGE):se(n.SAMPLE_ALPHA_TO_COVERAGE)}function We(R){q!==R&&(R?n.frontFace(n.CW):n.frontFace(n.CCW),q=R)}function Ft(R){R!==gx?(ne(n.CULL_FACE),R!==C&&(R===Ym?n.cullFace(n.BACK):R===vx?n.cullFace(n.FRONT):n.cullFace(n.FRONT_AND_BACK))):se(n.CULL_FACE),C=R}function w(R){R!==U&&(H&&n.lineWidth(R),U=R)}function Vt(R,re,te){R?(ne(n.POLYGON_OFFSET_FILL),(V!==re||W!==te)&&(V=re,W=te,o.getReversed()&&(re=-re),n.polygonOffset(re,te))):se(n.POLYGON_OFFSET_FILL)}function lt(R){R?ne(n.SCISSOR_TEST):se(n.SCISSOR_TEST)}function bt(R){R===void 0&&(R=n.TEXTURE0+B-1),Z!==R&&(n.activeTexture(R),Z=R)}function be(R,re,te){te===void 0&&(Z===null?te=n.TEXTURE0+B-1:te=Z);let me=ue[te];me===void 0&&(me={type:void 0,texture:void 0},ue[te]=me),(me.type!==R||me.texture!==re)&&(Z!==te&&(n.activeTexture(te),Z=te),n.bindTexture(R,re||X[R]),me.type=R,me.texture=re)}function b(){let R=ue[Z];R!==void 0&&R.type!==void 0&&(n.bindTexture(R.type,null),R.type=void 0,R.texture=void 0)}function v(){try{n.compressedTexImage2D(...arguments)}catch(R){Ne("WebGLState:",R)}}function N(){try{n.compressedTexImage3D(...arguments)}catch(R){Ne("WebGLState:",R)}}function $(){try{n.texSubImage2D(...arguments)}catch(R){Ne("WebGLState:",R)}}function Y(){try{n.texSubImage3D(...arguments)}catch(R){Ne("WebGLState:",R)}}function j(){try{n.compressedTexSubImage2D(...arguments)}catch(R){Ne("WebGLState:",R)}}function ve(){try{n.compressedTexSubImage3D(...arguments)}catch(R){Ne("WebGLState:",R)}}function ie(){try{n.texStorage2D(...arguments)}catch(R){Ne("WebGLState:",R)}}function Ce(){try{n.texStorage3D(...arguments)}catch(R){Ne("WebGLState:",R)}}function Re(){try{n.texImage2D(...arguments)}catch(R){Ne("WebGLState:",R)}}function J(){try{n.texImage3D(...arguments)}catch(R){Ne("WebGLState:",R)}}function ee(R){ze.equals(R)===!1&&(n.scissor(R.x,R.y,R.z,R.w),ze.copy(R))}function ye(R){Tt.equals(R)===!1&&(n.viewport(R.x,R.y,R.z,R.w),Tt.copy(R))}function xe(R,re){let te=l.get(re);te===void 0&&(te=new WeakMap,l.set(re,te));let me=te.get(R);me===void 0&&(me=n.getUniformBlockIndex(re,R.name),te.set(R,me))}function de(R,re){let me=l.get(re).get(R);c.get(re)!==me&&(n.uniformBlockBinding(re,me,R.__bindingPointIndex),c.set(re,me))}function je(){n.disable(n.BLEND),n.disable(n.CULL_FACE),n.disable(n.DEPTH_TEST),n.disable(n.POLYGON_OFFSET_FILL),n.disable(n.SCISSOR_TEST),n.disable(n.STENCIL_TEST),n.disable(n.SAMPLE_ALPHA_TO_COVERAGE),n.blendEquation(n.FUNC_ADD),n.blendFunc(n.ONE,n.ZERO),n.blendFuncSeparate(n.ONE,n.ZERO,n.ONE,n.ZERO),n.blendColor(0,0,0,0),n.colorMask(!0,!0,!0,!0),n.clearColor(0,0,0,0),n.depthMask(!0),n.depthFunc(n.LESS),o.setReversed(!1),n.clearDepth(1),n.stencilMask(4294967295),n.stencilFunc(n.ALWAYS,0,4294967295),n.stencilOp(n.KEEP,n.KEEP,n.KEEP),n.clearStencil(0),n.cullFace(n.BACK),n.frontFace(n.CCW),n.polygonOffset(0,0),n.activeTexture(n.TEXTURE0),n.bindFramebuffer(n.FRAMEBUFFER,null),n.bindFramebuffer(n.DRAW_FRAMEBUFFER,null),n.bindFramebuffer(n.READ_FRAMEBUFFER,null),n.useProgram(null),n.lineWidth(1),n.scissor(0,0,n.canvas.width,n.canvas.height),n.viewport(0,0,n.canvas.width,n.canvas.height),u={},Z=null,ue={},f={},d=new WeakMap,h=[],g=null,x=!1,m=null,p=null,M=null,T=null,S=null,I=null,D=null,A=new Xe(0,0,0),y=0,E=!1,q=null,C=null,U=null,V=null,W=null,ze.set(0,0,n.canvas.width,n.canvas.height),Tt.set(0,0,n.canvas.width,n.canvas.height),s.reset(),o.reset(),a.reset()}return{buffers:{color:s,depth:o,stencil:a},enable:ne,disable:se,bindFramebuffer:Ve,drawBuffers:Ie,useProgram:Fe,setBlending:ft,setMaterial:_t,setFlipSided:We,setCullFace:Ft,setLineWidth:w,setPolygonOffset:Vt,setScissorTest:lt,activeTexture:bt,bindTexture:be,unbindTexture:b,compressedTexImage2D:v,compressedTexImage3D:N,texImage2D:Re,texImage3D:J,updateUBOMapping:xe,uniformBlockBinding:de,texStorage2D:ie,texStorage3D:Ce,texSubImage2D:$,texSubImage3D:Y,compressedTexSubImage2D:j,compressedTexSubImage3D:ve,scissor:ee,viewport:ye,reset:je}}function kR(n,e,t,i,r,s,o){let a=e.has("WEBGL_multisampled_render_to_texture")?e.get("WEBGL_multisampled_render_to_texture"):null,c=typeof navigator>"u"?!1:/OculusBrowser/g.test(navigator.userAgent),l=new pt,u=new WeakMap,f,d=new WeakMap,h=!1;try{h=typeof OffscreenCanvas<"u"&&new OffscreenCanvas(1,1).getContext("2d")!==null}catch{}function g(b,v){return h?new OffscreenCanvas(b,v):Va("canvas")}function x(b,v,N){let $=1,Y=be(b);if((Y.width>N||Y.height>N)&&($=N/Math.max(Y.width,Y.height)),$<1)if(typeof HTMLImageElement<"u"&&b instanceof HTMLImageElement||typeof HTMLCanvasElement<"u"&&b instanceof HTMLCanvasElement||typeof ImageBitmap<"u"&&b instanceof ImageBitmap||typeof VideoFrame<"u"&&b instanceof VideoFrame){let j=Math.floor($*Y.width),ve=Math.floor($*Y.height);f===void 0&&(f=g(j,ve));let ie=v?g(j,ve):f;return ie.width=j,ie.height=ve,ie.getContext("2d").drawImage(b,0,0,j,ve),Le("WebGLRenderer: Texture has been resized from ("+Y.width+"x"+Y.height+") to ("+j+"x"+ve+")."),ie}else return"data"in b&&Le("WebGLRenderer: Image in DataTexture is too big ("+Y.width+"x"+Y.height+")."),b;return b}function m(b){return b.generateMipmaps}function p(b){n.generateMipmap(b)}function M(b){return b.isWebGLCubeRenderTarget?n.TEXTURE_CUBE_MAP:b.isWebGL3DRenderTarget?n.TEXTURE_3D:b.isWebGLArrayRenderTarget||b.isCompressedArrayTexture?n.TEXTURE_2D_ARRAY:n.TEXTURE_2D}function T(b,v,N,$,Y=!1){if(b!==null){if(n[b]!==void 0)return n[b];Le("WebGLRenderer: Attempt to use non-existing WebGL internal format '"+b+"'")}let j=v;if(v===n.RED&&(N===n.FLOAT&&(j=n.R32F),N===n.HALF_FLOAT&&(j=n.R16F),N===n.UNSIGNED_BYTE&&(j=n.R8)),v===n.RED_INTEGER&&(N===n.UNSIGNED_BYTE&&(j=n.R8UI),N===n.UNSIGNED_SHORT&&(j=n.R16UI),N===n.UNSIGNED_INT&&(j=n.R32UI),N===n.BYTE&&(j=n.R8I),N===n.SHORT&&(j=n.R16I),N===n.INT&&(j=n.R32I)),v===n.RG&&(N===n.FLOAT&&(j=n.RG32F),N===n.HALF_FLOAT&&(j=n.RG16F),N===n.UNSIGNED_BYTE&&(j=n.RG8)),v===n.RG_INTEGER&&(N===n.UNSIGNED_BYTE&&(j=n.RG8UI),N===n.UNSIGNED_SHORT&&(j=n.RG16UI),N===n.UNSIGNED_INT&&(j=n.RG32UI),N===n.BYTE&&(j=n.RG8I),N===n.SHORT&&(j=n.RG16I),N===n.INT&&(j=n.RG32I)),v===n.RGB_INTEGER&&(N===n.UNSIGNED_BYTE&&(j=n.RGB8UI),N===n.UNSIGNED_SHORT&&(j=n.RGB16UI),N===n.UNSIGNED_INT&&(j=n.RGB32UI),N===n.BYTE&&(j=n.RGB8I),N===n.SHORT&&(j=n.RGB16I),N===n.INT&&(j=n.RGB32I)),v===n.RGBA_INTEGER&&(N===n.UNSIGNED_BYTE&&(j=n.RGBA8UI),N===n.UNSIGNED_SHORT&&(j=n.RGBA16UI),N===n.UNSIGNED_INT&&(j=n.RGBA32UI),N===n.BYTE&&(j=n.RGBA8I),N===n.SHORT&&(j=n.RGBA16I),N===n.INT&&(j=n.RGBA32I)),v===n.RGB&&(N===n.UNSIGNED_INT_5_9_9_9_REV&&(j=n.RGB9_E5),N===n.UNSIGNED_INT_10F_11F_11F_REV&&(j=n.R11F_G11F_B10F)),v===n.RGBA){let ve=Y?Ua:rt.getTransfer($);N===n.FLOAT&&(j=n.RGBA32F),N===n.HALF_FLOAT&&(j=n.RGBA16F),N===n.UNSIGNED_BYTE&&(j=ve===dt?n.SRGB8_ALPHA8:n.RGBA8),N===n.UNSIGNED_SHORT_4_4_4_4&&(j=n.RGBA4),N===n.UNSIGNED_SHORT_5_5_5_1&&(j=n.RGB5_A1)}return(j===n.R16F||j===n.R32F||j===n.RG16F||j===n.RG32F||j===n.RGBA16F||j===n.RGBA32F)&&e.get("EXT_color_buffer_float"),j}function S(b,v){let N;return b?v===null||v===mi||v===Po?N=n.DEPTH24_STENCIL8:v===gi?N=n.DEPTH32F_STENCIL8:v===No&&(N=n.DEPTH24_STENCIL8,Le("DepthTexture: 16 bit depth attachment is not supported with stencil. Using 24-bit attachment.")):v===null||v===mi||v===Po?N=n.DEPTH_COMPONENT24:v===gi?N=n.DEPTH_COMPONENT32F:v===No&&(N=n.DEPTH_COMPONENT16),N}function I(b,v){return m(b)===!0||b.isFramebufferTexture&&b.minFilter!==en&&b.minFilter!==sn?Math.log2(Math.max(v.width,v.height))+1:b.mipmaps!==void 0&&b.mipmaps.length>0?b.mipmaps.length:b.isCompressedTexture&&Array.isArray(b.image)?v.mipmaps.length:1}function D(b){let v=b.target;v.removeEventListener("dispose",D),y(v),v.isVideoTexture&&u.delete(v)}function A(b){let v=b.target;v.removeEventListener("dispose",A),q(v)}function y(b){let v=i.get(b);if(v.__webglInit===void 0)return;let N=b.source,$=d.get(N);if($){let Y=$[v.__cacheKey];Y.usedTimes--,Y.usedTimes===0&&E(b),Object.keys($).length===0&&d.delete(N)}i.remove(b)}function E(b){let v=i.get(b);n.deleteTexture(v.__webglTexture);let N=b.source,$=d.get(N);delete $[v.__cacheKey],o.memory.textures--}function q(b){let v=i.get(b);if(b.depthTexture&&(b.depthTexture.dispose(),i.remove(b.depthTexture)),b.isWebGLCubeRenderTarget)for(let $=0;$<6;$++){if(Array.isArray(v.__webglFramebuffer[$]))for(let Y=0;Y<v.__webglFramebuffer[$].length;Y++)n.deleteFramebuffer(v.__webglFramebuffer[$][Y]);else n.deleteFramebuffer(v.__webglFramebuffer[$]);v.__webglDepthbuffer&&n.deleteRenderbuffer(v.__webglDepthbuffer[$])}else{if(Array.isArray(v.__webglFramebuffer))for(let $=0;$<v.__webglFramebuffer.length;$++)n.deleteFramebuffer(v.__webglFramebuffer[$]);else n.deleteFramebuffer(v.__webglFramebuffer);if(v.__webglDepthbuffer&&n.deleteRenderbuffer(v.__webglDepthbuffer),v.__webglMultisampledFramebuffer&&n.deleteFramebuffer(v.__webglMultisampledFramebuffer),v.__webglColorRenderbuffer)for(let $=0;$<v.__webglColorRenderbuffer.length;$++)v.__webglColorRenderbuffer[$]&&n.deleteRenderbuffer(v.__webglColorRenderbuffer[$]);v.__webglDepthRenderbuffer&&n.deleteRenderbuffer(v.__webglDepthRenderbuffer)}let N=b.textures;for(let $=0,Y=N.length;$<Y;$++){let j=i.get(N[$]);j.__webglTexture&&(n.deleteTexture(j.__webglTexture),o.memory.textures--),i.remove(N[$])}i.remove(b)}let C=0;function U(){C=0}function V(){let b=C;return b>=r.maxTextures&&Le("WebGLTextures: Trying to use "+b+" texture units while this GPU supports only "+r.maxTextures),C+=1,b}function W(b){let v=[];return v.push(b.wrapS),v.push(b.wrapT),v.push(b.wrapR||0),v.push(b.magFilter),v.push(b.minFilter),v.push(b.anisotropy),v.push(b.internalFormat),v.push(b.format),v.push(b.type),v.push(b.generateMipmaps),v.push(b.premultiplyAlpha),v.push(b.flipY),v.push(b.unpackAlignment),v.push(b.colorSpace),v.join()}function B(b,v){let N=i.get(b);if(b.isVideoTexture&&lt(b),b.isRenderTargetTexture===!1&&b.isExternalTexture!==!0&&b.version>0&&N.__version!==b.version){let $=b.image;if($===null)Le("WebGLRenderer: Texture marked for update but no image data found.");else if($.complete===!1)Le("WebGLRenderer: Texture marked for update but image is incomplete");else{X(N,b,v);return}}else b.isExternalTexture&&(N.__webglTexture=b.sourceTexture?b.sourceTexture:null);t.bindTexture(n.TEXTURE_2D,N.__webglTexture,n.TEXTURE0+v)}function H(b,v){let N=i.get(b);if(b.isRenderTargetTexture===!1&&b.version>0&&N.__version!==b.version){X(N,b,v);return}else b.isExternalTexture&&(N.__webglTexture=b.sourceTexture?b.sourceTexture:null);t.bindTexture(n.TEXTURE_2D_ARRAY,N.__webglTexture,n.TEXTURE0+v)}function L(b,v){let N=i.get(b);if(b.isRenderTargetTexture===!1&&b.version>0&&N.__version!==b.version){X(N,b,v);return}t.bindTexture(n.TEXTURE_3D,N.__webglTexture,n.TEXTURE0+v)}function Q(b,v){let N=i.get(b);if(b.isCubeDepthTexture!==!0&&b.version>0&&N.__version!==b.version){ne(N,b,v);return}t.bindTexture(n.TEXTURE_CUBE_MAP,N.__webglTexture,n.TEXTURE0+v)}let Z={[Ou]:n.REPEAT,[Ci]:n.CLAMP_TO_EDGE,[ku]:n.MIRRORED_REPEAT},ue={[en]:n.NEAREST,[Bx]:n.NEAREST_MIPMAP_NEAREST,[oc]:n.NEAREST_MIPMAP_LINEAR,[sn]:n.LINEAR,[ad]:n.LINEAR_MIPMAP_NEAREST,[Vr]:n.LINEAR_MIPMAP_LINEAR},ge={[Gx]:n.NEVER,[Xx]:n.ALWAYS,[Wx]:n.LESS,[$d]:n.LEQUAL,[jx]:n.EQUAL,[qd]:n.GEQUAL,[$x]:n.GREATER,[qx]:n.NOTEQUAL};function fe(b,v){if(v.type===gi&&e.has("OES_texture_float_linear")===!1&&(v.magFilter===sn||v.magFilter===ad||v.magFilter===oc||v.magFilter===Vr||v.minFilter===sn||v.minFilter===ad||v.minFilter===oc||v.minFilter===Vr)&&Le("WebGLRenderer: Unable to use linear filtering with floating point textures. OES_texture_float_linear not supported on this device."),n.texParameteri(b,n.TEXTURE_WRAP_S,Z[v.wrapS]),n.texParameteri(b,n.TEXTURE_WRAP_T,Z[v.wrapT]),(b===n.TEXTURE_3D||b===n.TEXTURE_2D_ARRAY)&&n.texParameteri(b,n.TEXTURE_WRAP_R,Z[v.wrapR]),n.texParameteri(b,n.TEXTURE_MAG_FILTER,ue[v.magFilter]),n.texParameteri(b,n.TEXTURE_MIN_FILTER,ue[v.minFilter]),v.compareFunction&&(n.texParameteri(b,n.TEXTURE_COMPARE_MODE,n.COMPARE_REF_TO_TEXTURE),n.texParameteri(b,n.TEXTURE_COMPARE_FUNC,ge[v.compareFunction])),e.has("EXT_texture_filter_anisotropic")===!0){if(v.magFilter===en||v.minFilter!==oc&&v.minFilter!==Vr||v.type===gi&&e.has("OES_texture_float_linear")===!1)return;if(v.anisotropy>1||i.get(v).__currentAnisotropy){let N=e.get("EXT_texture_filter_anisotropic");n.texParameterf(b,N.TEXTURE_MAX_ANISOTROPY_EXT,Math.min(v.anisotropy,r.getMaxAnisotropy())),i.get(v).__currentAnisotropy=v.anisotropy}}}function ze(b,v){let N=!1;b.__webglInit===void 0&&(b.__webglInit=!0,v.addEventListener("dispose",D));let $=v.source,Y=d.get($);Y===void 0&&(Y={},d.set($,Y));let j=W(v);if(j!==b.__cacheKey){Y[j]===void 0&&(Y[j]={texture:n.createTexture(),usedTimes:0},o.memory.textures++,N=!0),Y[j].usedTimes++;let ve=Y[b.__cacheKey];ve!==void 0&&(Y[b.__cacheKey].usedTimes--,ve.usedTimes===0&&E(v)),b.__cacheKey=j,b.__webglTexture=Y[j].texture}return N}function Tt(b,v,N){return Math.floor(Math.floor(b/N)/v)}function St(b,v,N,$){let j=b.updateRanges;if(j.length===0)t.texSubImage2D(n.TEXTURE_2D,0,0,0,v.width,v.height,N,$,v.data);else{j.sort((J,ee)=>J.start-ee.start);let ve=0;for(let J=1;J<j.length;J++){let ee=j[ve],ye=j[J],xe=ee.start+ee.count,de=Tt(ye.start,v.width,4),je=Tt(ee.start,v.width,4);ye.start<=xe+1&&de===je&&Tt(ye.start+ye.count-1,v.width,4)===de?ee.count=Math.max(ee.count,ye.start+ye.count-ee.start):(++ve,j[ve]=ye)}j.length=ve+1;let ie=n.getParameter(n.UNPACK_ROW_LENGTH),Ce=n.getParameter(n.UNPACK_SKIP_PIXELS),Re=n.getParameter(n.UNPACK_SKIP_ROWS);n.pixelStorei(n.UNPACK_ROW_LENGTH,v.width);for(let J=0,ee=j.length;J<ee;J++){let ye=j[J],xe=Math.floor(ye.start/4),de=Math.ceil(ye.count/4),je=xe%v.width,R=Math.floor(xe/v.width),re=de,te=1;n.pixelStorei(n.UNPACK_SKIP_PIXELS,je),n.pixelStorei(n.UNPACK_SKIP_ROWS,R),t.texSubImage2D(n.TEXTURE_2D,0,je,R,re,te,N,$,v.data)}b.clearUpdateRanges(),n.pixelStorei(n.UNPACK_ROW_LENGTH,ie),n.pixelStorei(n.UNPACK_SKIP_PIXELS,Ce),n.pixelStorei(n.UNPACK_SKIP_ROWS,Re)}}function X(b,v,N){let $=n.TEXTURE_2D;(v.isDataArrayTexture||v.isCompressedArrayTexture)&&($=n.TEXTURE_2D_ARRAY),v.isData3DTexture&&($=n.TEXTURE_3D);let Y=ze(b,v),j=v.source;t.bindTexture($,b.__webglTexture,n.TEXTURE0+N);let ve=i.get(j);if(j.version!==ve.__version||Y===!0){t.activeTexture(n.TEXTURE0+N);let ie=rt.getPrimaries(rt.workingColorSpace),Ce=v.colorSpace===or?null:rt.getPrimaries(v.colorSpace),Re=v.colorSpace===or||ie===Ce?n.NONE:n.BROWSER_DEFAULT_WEBGL;n.pixelStorei(n.UNPACK_FLIP_Y_WEBGL,v.flipY),n.pixelStorei(n.UNPACK_PREMULTIPLY_ALPHA_WEBGL,v.premultiplyAlpha),n.pixelStorei(n.UNPACK_ALIGNMENT,v.unpackAlignment),n.pixelStorei(n.UNPACK_COLORSPACE_CONVERSION_WEBGL,Re);let J=x(v.image,!1,r.maxTextureSize);J=bt(v,J);let ee=s.convert(v.format,v.colorSpace),ye=s.convert(v.type),xe=T(v.internalFormat,ee,ye,v.colorSpace,v.isVideoTexture);fe($,v);let de,je=v.mipmaps,R=v.isVideoTexture!==!0,re=ve.__version===void 0||Y===!0,te=j.dataReady,me=I(v,J);if(v.isDepthTexture)xe=S(v.format===Hr,v.type),re&&(R?t.texStorage2D(n.TEXTURE_2D,1,xe,J.width,J.height):t.texImage2D(n.TEXTURE_2D,0,xe,J.width,J.height,0,ee,ye,null));else if(v.isDataTexture)if(je.length>0){R&&re&&t.texStorage2D(n.TEXTURE_2D,me,xe,je[0].width,je[0].height);for(let K=0,G=je.length;K<G;K++)de=je[K],R?te&&t.texSubImage2D(n.TEXTURE_2D,K,0,0,de.width,de.height,ee,ye,de.data):t.texImage2D(n.TEXTURE_2D,K,xe,de.width,de.height,0,ee,ye,de.data);v.generateMipmaps=!1}else R?(re&&t.texStorage2D(n.TEXTURE_2D,me,xe,J.width,J.height),te&&St(v,J,ee,ye)):t.texImage2D(n.TEXTURE_2D,0,xe,J.width,J.height,0,ee,ye,J.data);else if(v.isCompressedTexture)if(v.isCompressedArrayTexture){R&&re&&t.texStorage3D(n.TEXTURE_2D_ARRAY,me,xe,je[0].width,je[0].height,J.depth);for(let K=0,G=je.length;K<G;K++)if(de=je[K],v.format!==Jn)if(ee!==null)if(R){if(te)if(v.layerUpdates.size>0){let _e=_g(de.width,de.height,v.format,v.type);for(let ke of v.layerUpdates){let Et=de.data.subarray(ke*_e/de.data.BYTES_PER_ELEMENT,(ke+1)*_e/de.data.BYTES_PER_ELEMENT);t.compressedTexSubImage3D(n.TEXTURE_2D_ARRAY,K,0,0,ke,de.width,de.height,1,ee,Et)}v.clearLayerUpdates()}else t.compressedTexSubImage3D(n.TEXTURE_2D_ARRAY,K,0,0,0,de.width,de.height,J.depth,ee,de.data)}else t.compressedTexImage3D(n.TEXTURE_2D_ARRAY,K,xe,de.width,de.height,J.depth,0,de.data,0,0);else Le("WebGLRenderer: Attempt to load unsupported compressed texture format in .uploadTexture()");else R?te&&t.texSubImage3D(n.TEXTURE_2D_ARRAY,K,0,0,0,de.width,de.height,J.depth,ee,ye,de.data):t.texImage3D(n.TEXTURE_2D_ARRAY,K,xe,de.width,de.height,J.depth,0,ee,ye,de.data)}else{R&&re&&t.texStorage2D(n.TEXTURE_2D,me,xe,je[0].width,je[0].height);for(let K=0,G=je.length;K<G;K++)de=je[K],v.format!==Jn?ee!==null?R?te&&t.compressedTexSubImage2D(n.TEXTURE_2D,K,0,0,de.width,de.height,ee,de.data):t.compressedTexImage2D(n.TEXTURE_2D,K,xe,de.width,de.height,0,de.data):Le("WebGLRenderer: Attempt to load unsupported compressed texture format in .uploadTexture()"):R?te&&t.texSubImage2D(n.TEXTURE_2D,K,0,0,de.width,de.height,ee,ye,de.data):t.texImage2D(n.TEXTURE_2D,K,xe,de.width,de.height,0,ee,ye,de.data)}else if(v.isDataArrayTexture)if(R){if(re&&t.texStorage3D(n.TEXTURE_2D_ARRAY,me,xe,J.width,J.height,J.depth),te)if(v.layerUpdates.size>0){let K=_g(J.width,J.height,v.format,v.type);for(let G of v.layerUpdates){let _e=J.data.subarray(G*K/J.data.BYTES_PER_ELEMENT,(G+1)*K/J.data.BYTES_PER_ELEMENT);t.texSubImage3D(n.TEXTURE_2D_ARRAY,0,0,0,G,J.width,J.height,1,ee,ye,_e)}v.clearLayerUpdates()}else t.texSubImage3D(n.TEXTURE_2D_ARRAY,0,0,0,0,J.width,J.height,J.depth,ee,ye,J.data)}else t.texImage3D(n.TEXTURE_2D_ARRAY,0,xe,J.width,J.height,J.depth,0,ee,ye,J.data);else if(v.isData3DTexture)R?(re&&t.texStorage3D(n.TEXTURE_3D,me,xe,J.width,J.height,J.depth),te&&t.texSubImage3D(n.TEXTURE_3D,0,0,0,0,J.width,J.height,J.depth,ee,ye,J.data)):t.texImage3D(n.TEXTURE_3D,0,xe,J.width,J.height,J.depth,0,ee,ye,J.data);else if(v.isFramebufferTexture){if(re)if(R)t.texStorage2D(n.TEXTURE_2D,me,xe,J.width,J.height);else{let K=J.width,G=J.height;for(let _e=0;_e<me;_e++)t.texImage2D(n.TEXTURE_2D,_e,xe,K,G,0,ee,ye,null),K>>=1,G>>=1}}else if(je.length>0){if(R&&re){let K=be(je[0]);t.texStorage2D(n.TEXTURE_2D,me,xe,K.width,K.height)}for(let K=0,G=je.length;K<G;K++)de=je[K],R?te&&t.texSubImage2D(n.TEXTURE_2D,K,0,0,ee,ye,de):t.texImage2D(n.TEXTURE_2D,K,xe,ee,ye,de);v.generateMipmaps=!1}else if(R){if(re){let K=be(J);t.texStorage2D(n.TEXTURE_2D,me,xe,K.width,K.height)}te&&t.texSubImage2D(n.TEXTURE_2D,0,0,0,ee,ye,J)}else t.texImage2D(n.TEXTURE_2D,0,xe,ee,ye,J);m(v)&&p($),ve.__version=j.version,v.onUpdate&&v.onUpdate(v)}b.__version=v.version}function ne(b,v,N){if(v.image.length!==6)return;let $=ze(b,v),Y=v.source;t.bindTexture(n.TEXTURE_CUBE_MAP,b.__webglTexture,n.TEXTURE0+N);let j=i.get(Y);if(Y.version!==j.__version||$===!0){t.activeTexture(n.TEXTURE0+N);let ve=rt.getPrimaries(rt.workingColorSpace),ie=v.colorSpace===or?null:rt.getPrimaries(v.colorSpace),Ce=v.colorSpace===or||ve===ie?n.NONE:n.BROWSER_DEFAULT_WEBGL;n.pixelStorei(n.UNPACK_FLIP_Y_WEBGL,v.flipY),n.pixelStorei(n.UNPACK_PREMULTIPLY_ALPHA_WEBGL,v.premultiplyAlpha),n.pixelStorei(n.UNPACK_ALIGNMENT,v.unpackAlignment),n.pixelStorei(n.UNPACK_COLORSPACE_CONVERSION_WEBGL,Ce);let Re=v.isCompressedTexture||v.image[0].isCompressedTexture,J=v.image[0]&&v.image[0].isDataTexture,ee=[];for(let G=0;G<6;G++)!Re&&!J?ee[G]=x(v.image[G],!0,r.maxCubemapSize):ee[G]=J?v.image[G].image:v.image[G],ee[G]=bt(v,ee[G]);let ye=ee[0],xe=s.convert(v.format,v.colorSpace),de=s.convert(v.type),je=T(v.internalFormat,xe,de,v.colorSpace),R=v.isVideoTexture!==!0,re=j.__version===void 0||$===!0,te=Y.dataReady,me=I(v,ye);fe(n.TEXTURE_CUBE_MAP,v);let K;if(Re){R&&re&&t.texStorage2D(n.TEXTURE_CUBE_MAP,me,je,ye.width,ye.height);for(let G=0;G<6;G++){K=ee[G].mipmaps;for(let _e=0;_e<K.length;_e++){let ke=K[_e];v.format!==Jn?xe!==null?R?te&&t.compressedTexSubImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+G,_e,0,0,ke.width,ke.height,xe,ke.data):t.compressedTexImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+G,_e,je,ke.width,ke.height,0,ke.data):Le("WebGLRenderer: Attempt to load unsupported compressed texture format in .setTextureCube()"):R?te&&t.texSubImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+G,_e,0,0,ke.width,ke.height,xe,de,ke.data):t.texImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+G,_e,je,ke.width,ke.height,0,xe,de,ke.data)}}}else{if(K=v.mipmaps,R&&re){K.length>0&&me++;let G=be(ee[0]);t.texStorage2D(n.TEXTURE_CUBE_MAP,me,je,G.width,G.height)}for(let G=0;G<6;G++)if(J){R?te&&t.texSubImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+G,0,0,0,ee[G].width,ee[G].height,xe,de,ee[G].data):t.texImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+G,0,je,ee[G].width,ee[G].height,0,xe,de,ee[G].data);for(let _e=0;_e<K.length;_e++){let Et=K[_e].image[G].image;R?te&&t.texSubImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+G,_e+1,0,0,Et.width,Et.height,xe,de,Et.data):t.texImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+G,_e+1,je,Et.width,Et.height,0,xe,de,Et.data)}}else{R?te&&t.texSubImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+G,0,0,0,xe,de,ee[G]):t.texImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+G,0,je,xe,de,ee[G]);for(let _e=0;_e<K.length;_e++){let ke=K[_e];R?te&&t.texSubImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+G,_e+1,0,0,xe,de,ke.image[G]):t.texImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+G,_e+1,je,xe,de,ke.image[G])}}}m(v)&&p(n.TEXTURE_CUBE_MAP),j.__version=Y.version,v.onUpdate&&v.onUpdate(v)}b.__version=v.version}function se(b,v,N,$,Y,j){let ve=s.convert(N.format,N.colorSpace),ie=s.convert(N.type),Ce=T(N.internalFormat,ve,ie,N.colorSpace),Re=i.get(v),J=i.get(N);if(J.__renderTarget=v,!Re.__hasExternalTextures){let ee=Math.max(1,v.width>>j),ye=Math.max(1,v.height>>j);Y===n.TEXTURE_3D||Y===n.TEXTURE_2D_ARRAY?t.texImage3D(Y,j,Ce,ee,ye,v.depth,0,ve,ie,null):t.texImage2D(Y,j,Ce,ee,ye,0,ve,ie,null)}t.bindFramebuffer(n.FRAMEBUFFER,b),Vt(v)?a.framebufferTexture2DMultisampleEXT(n.FRAMEBUFFER,$,Y,J.__webglTexture,0,w(v)):(Y===n.TEXTURE_2D||Y>=n.TEXTURE_CUBE_MAP_POSITIVE_X&&Y<=n.TEXTURE_CUBE_MAP_NEGATIVE_Z)&&n.framebufferTexture2D(n.FRAMEBUFFER,$,Y,J.__webglTexture,j),t.bindFramebuffer(n.FRAMEBUFFER,null)}function Ve(b,v,N){if(n.bindRenderbuffer(n.RENDERBUFFER,b),v.depthBuffer){let $=v.depthTexture,Y=$&&$.isDepthTexture?$.type:null,j=S(v.stencilBuffer,Y),ve=v.stencilBuffer?n.DEPTH_STENCIL_ATTACHMENT:n.DEPTH_ATTACHMENT;Vt(v)?a.renderbufferStorageMultisampleEXT(n.RENDERBUFFER,w(v),j,v.width,v.height):N?n.renderbufferStorageMultisample(n.RENDERBUFFER,w(v),j,v.width,v.height):n.renderbufferStorage(n.RENDERBUFFER,j,v.width,v.height),n.framebufferRenderbuffer(n.FRAMEBUFFER,ve,n.RENDERBUFFER,b)}else{let $=v.textures;for(let Y=0;Y<$.length;Y++){let j=$[Y],ve=s.convert(j.format,j.colorSpace),ie=s.convert(j.type),Ce=T(j.internalFormat,ve,ie,j.colorSpace);Vt(v)?a.renderbufferStorageMultisampleEXT(n.RENDERBUFFER,w(v),Ce,v.width,v.height):N?n.renderbufferStorageMultisample(n.RENDERBUFFER,w(v),Ce,v.width,v.height):n.renderbufferStorage(n.RENDERBUFFER,Ce,v.width,v.height)}}n.bindRenderbuffer(n.RENDERBUFFER,null)}function Ie(b,v,N){let $=v.isWebGLCubeRenderTarget===!0;if(t.bindFramebuffer(n.FRAMEBUFFER,b),!(v.depthTexture&&v.depthTexture.isDepthTexture))throw new Error("renderTarget.depthTexture must be an instance of THREE.DepthTexture");let Y=i.get(v.depthTexture);if(Y.__renderTarget=v,(!Y.__webglTexture||v.depthTexture.image.width!==v.width||v.depthTexture.image.height!==v.height)&&(v.depthTexture.image.width=v.width,v.depthTexture.image.height=v.height,v.depthTexture.needsUpdate=!0),$){if(Y.__webglInit===void 0&&(Y.__webglInit=!0,v.depthTexture.addEventListener("dispose",D)),Y.__webglTexture===void 0){Y.__webglTexture=n.createTexture(),t.bindTexture(n.TEXTURE_CUBE_MAP,Y.__webglTexture),fe(n.TEXTURE_CUBE_MAP,v.depthTexture);let Re=s.convert(v.depthTexture.format),J=s.convert(v.depthTexture.type),ee;v.depthTexture.format===wi?ee=n.DEPTH_COMPONENT24:v.depthTexture.format===Hr&&(ee=n.DEPTH24_STENCIL8);for(let ye=0;ye<6;ye++)n.texImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+ye,0,ee,v.width,v.height,0,Re,J,null)}}else B(v.depthTexture,0);let j=Y.__webglTexture,ve=w(v),ie=$?n.TEXTURE_CUBE_MAP_POSITIVE_X+N:n.TEXTURE_2D,Ce=v.depthTexture.format===Hr?n.DEPTH_STENCIL_ATTACHMENT:n.DEPTH_ATTACHMENT;if(v.depthTexture.format===wi)Vt(v)?a.framebufferTexture2DMultisampleEXT(n.FRAMEBUFFER,Ce,ie,j,0,ve):n.framebufferTexture2D(n.FRAMEBUFFER,Ce,ie,j,0);else if(v.depthTexture.format===Hr)Vt(v)?a.framebufferTexture2DMultisampleEXT(n.FRAMEBUFFER,Ce,ie,j,0,ve):n.framebufferTexture2D(n.FRAMEBUFFER,Ce,ie,j,0);else throw new Error("Unknown depthTexture format")}function Fe(b){let v=i.get(b),N=b.isWebGLCubeRenderTarget===!0;if(v.__boundDepthTexture!==b.depthTexture){let $=b.depthTexture;if(v.__depthDisposeCallback&&v.__depthDisposeCallback(),$){let Y=()=>{delete v.__boundDepthTexture,delete v.__depthDisposeCallback,$.removeEventListener("dispose",Y)};$.addEventListener("dispose",Y),v.__depthDisposeCallback=Y}v.__boundDepthTexture=$}if(b.depthTexture&&!v.__autoAllocateDepthBuffer)if(N)for(let $=0;$<6;$++)Ie(v.__webglFramebuffer[$],b,$);else{let $=b.texture.mipmaps;$&&$.length>0?Ie(v.__webglFramebuffer[0],b,0):Ie(v.__webglFramebuffer,b,0)}else if(N){v.__webglDepthbuffer=[];for(let $=0;$<6;$++)if(t.bindFramebuffer(n.FRAMEBUFFER,v.__webglFramebuffer[$]),v.__webglDepthbuffer[$]===void 0)v.__webglDepthbuffer[$]=n.createRenderbuffer(),Ve(v.__webglDepthbuffer[$],b,!1);else{let Y=b.stencilBuffer?n.DEPTH_STENCIL_ATTACHMENT:n.DEPTH_ATTACHMENT,j=v.__webglDepthbuffer[$];n.bindRenderbuffer(n.RENDERBUFFER,j),n.framebufferRenderbuffer(n.FRAMEBUFFER,Y,n.RENDERBUFFER,j)}}else{let $=b.texture.mipmaps;if($&&$.length>0?t.bindFramebuffer(n.FRAMEBUFFER,v.__webglFramebuffer[0]):t.bindFramebuffer(n.FRAMEBUFFER,v.__webglFramebuffer),v.__webglDepthbuffer===void 0)v.__webglDepthbuffer=n.createRenderbuffer(),Ve(v.__webglDepthbuffer,b,!1);else{let Y=b.stencilBuffer?n.DEPTH_STENCIL_ATTACHMENT:n.DEPTH_ATTACHMENT,j=v.__webglDepthbuffer;n.bindRenderbuffer(n.RENDERBUFFER,j),n.framebufferRenderbuffer(n.FRAMEBUFFER,Y,n.RENDERBUFFER,j)}}t.bindFramebuffer(n.FRAMEBUFFER,null)}function Yt(b,v,N){let $=i.get(b);v!==void 0&&se($.__webglFramebuffer,b,b.texture,n.COLOR_ATTACHMENT0,n.TEXTURE_2D,0),N!==void 0&&Fe(b)}function st(b){let v=b.texture,N=i.get(b),$=i.get(v);b.addEventListener("dispose",A);let Y=b.textures,j=b.isWebGLCubeRenderTarget===!0,ve=Y.length>1;if(ve||($.__webglTexture===void 0&&($.__webglTexture=n.createTexture()),$.__version=v.version,o.memory.textures++),j){N.__webglFramebuffer=[];for(let ie=0;ie<6;ie++)if(v.mipmaps&&v.mipmaps.length>0){N.__webglFramebuffer[ie]=[];for(let Ce=0;Ce<v.mipmaps.length;Ce++)N.__webglFramebuffer[ie][Ce]=n.createFramebuffer()}else N.__webglFramebuffer[ie]=n.createFramebuffer()}else{if(v.mipmaps&&v.mipmaps.length>0){N.__webglFramebuffer=[];for(let ie=0;ie<v.mipmaps.length;ie++)N.__webglFramebuffer[ie]=n.createFramebuffer()}else N.__webglFramebuffer=n.createFramebuffer();if(ve)for(let ie=0,Ce=Y.length;ie<Ce;ie++){let Re=i.get(Y[ie]);Re.__webglTexture===void 0&&(Re.__webglTexture=n.createTexture(),o.memory.textures++)}if(b.samples>0&&Vt(b)===!1){N.__webglMultisampledFramebuffer=n.createFramebuffer(),N.__webglColorRenderbuffer=[],t.bindFramebuffer(n.FRAMEBUFFER,N.__webglMultisampledFramebuffer);for(let ie=0;ie<Y.length;ie++){let Ce=Y[ie];N.__webglColorRenderbuffer[ie]=n.createRenderbuffer(),n.bindRenderbuffer(n.RENDERBUFFER,N.__webglColorRenderbuffer[ie]);let Re=s.convert(Ce.format,Ce.colorSpace),J=s.convert(Ce.type),ee=T(Ce.internalFormat,Re,J,Ce.colorSpace,b.isXRRenderTarget===!0),ye=w(b);n.renderbufferStorageMultisample(n.RENDERBUFFER,ye,ee,b.width,b.height),n.framebufferRenderbuffer(n.FRAMEBUFFER,n.COLOR_ATTACHMENT0+ie,n.RENDERBUFFER,N.__webglColorRenderbuffer[ie])}n.bindRenderbuffer(n.RENDERBUFFER,null),b.depthBuffer&&(N.__webglDepthRenderbuffer=n.createRenderbuffer(),Ve(N.__webglDepthRenderbuffer,b,!0)),t.bindFramebuffer(n.FRAMEBUFFER,null)}}if(j){t.bindTexture(n.TEXTURE_CUBE_MAP,$.__webglTexture),fe(n.TEXTURE_CUBE_MAP,v);for(let ie=0;ie<6;ie++)if(v.mipmaps&&v.mipmaps.length>0)for(let Ce=0;Ce<v.mipmaps.length;Ce++)se(N.__webglFramebuffer[ie][Ce],b,v,n.COLOR_ATTACHMENT0,n.TEXTURE_CUBE_MAP_POSITIVE_X+ie,Ce);else se(N.__webglFramebuffer[ie],b,v,n.COLOR_ATTACHMENT0,n.TEXTURE_CUBE_MAP_POSITIVE_X+ie,0);m(v)&&p(n.TEXTURE_CUBE_MAP),t.unbindTexture()}else if(ve){for(let ie=0,Ce=Y.length;ie<Ce;ie++){let Re=Y[ie],J=i.get(Re),ee=n.TEXTURE_2D;(b.isWebGL3DRenderTarget||b.isWebGLArrayRenderTarget)&&(ee=b.isWebGL3DRenderTarget?n.TEXTURE_3D:n.TEXTURE_2D_ARRAY),t.bindTexture(ee,J.__webglTexture),fe(ee,Re),se(N.__webglFramebuffer,b,Re,n.COLOR_ATTACHMENT0+ie,ee,0),m(Re)&&p(ee)}t.unbindTexture()}else{let ie=n.TEXTURE_2D;if((b.isWebGL3DRenderTarget||b.isWebGLArrayRenderTarget)&&(ie=b.isWebGL3DRenderTarget?n.TEXTURE_3D:n.TEXTURE_2D_ARRAY),t.bindTexture(ie,$.__webglTexture),fe(ie,v),v.mipmaps&&v.mipmaps.length>0)for(let Ce=0;Ce<v.mipmaps.length;Ce++)se(N.__webglFramebuffer[Ce],b,v,n.COLOR_ATTACHMENT0,ie,Ce);else se(N.__webglFramebuffer,b,v,n.COLOR_ATTACHMENT0,ie,0);m(v)&&p(ie),t.unbindTexture()}b.depthBuffer&&Fe(b)}function ft(b){let v=b.textures;for(let N=0,$=v.length;N<$;N++){let Y=v[N];if(m(Y)){let j=M(b),ve=i.get(Y).__webglTexture;t.bindTexture(j,ve),p(j),t.unbindTexture()}}}let _t=[],We=[];function Ft(b){if(b.samples>0){if(Vt(b)===!1){let v=b.textures,N=b.width,$=b.height,Y=n.COLOR_BUFFER_BIT,j=b.stencilBuffer?n.DEPTH_STENCIL_ATTACHMENT:n.DEPTH_ATTACHMENT,ve=i.get(b),ie=v.length>1;if(ie)for(let Re=0;Re<v.length;Re++)t.bindFramebuffer(n.FRAMEBUFFER,ve.__webglMultisampledFramebuffer),n.framebufferRenderbuffer(n.FRAMEBUFFER,n.COLOR_ATTACHMENT0+Re,n.RENDERBUFFER,null),t.bindFramebuffer(n.FRAMEBUFFER,ve.__webglFramebuffer),n.framebufferTexture2D(n.DRAW_FRAMEBUFFER,n.COLOR_ATTACHMENT0+Re,n.TEXTURE_2D,null,0);t.bindFramebuffer(n.READ_FRAMEBUFFER,ve.__webglMultisampledFramebuffer);let Ce=b.texture.mipmaps;Ce&&Ce.length>0?t.bindFramebuffer(n.DRAW_FRAMEBUFFER,ve.__webglFramebuffer[0]):t.bindFramebuffer(n.DRAW_FRAMEBUFFER,ve.__webglFramebuffer);for(let Re=0;Re<v.length;Re++){if(b.resolveDepthBuffer&&(b.depthBuffer&&(Y|=n.DEPTH_BUFFER_BIT),b.stencilBuffer&&b.resolveStencilBuffer&&(Y|=n.STENCIL_BUFFER_BIT)),ie){n.framebufferRenderbuffer(n.READ_FRAMEBUFFER,n.COLOR_ATTACHMENT0,n.RENDERBUFFER,ve.__webglColorRenderbuffer[Re]);let J=i.get(v[Re]).__webglTexture;n.framebufferTexture2D(n.DRAW_FRAMEBUFFER,n.COLOR_ATTACHMENT0,n.TEXTURE_2D,J,0)}n.blitFramebuffer(0,0,N,$,0,0,N,$,Y,n.NEAREST),c===!0&&(_t.length=0,We.length=0,_t.push(n.COLOR_ATTACHMENT0+Re),b.depthBuffer&&b.resolveDepthBuffer===!1&&(_t.push(j),We.push(j),n.invalidateFramebuffer(n.DRAW_FRAMEBUFFER,We)),n.invalidateFramebuffer(n.READ_FRAMEBUFFER,_t))}if(t.bindFramebuffer(n.READ_FRAMEBUFFER,null),t.bindFramebuffer(n.DRAW_FRAMEBUFFER,null),ie)for(let Re=0;Re<v.length;Re++){t.bindFramebuffer(n.FRAMEBUFFER,ve.__webglMultisampledFramebuffer),n.framebufferRenderbuffer(n.FRAMEBUFFER,n.COLOR_ATTACHMENT0+Re,n.RENDERBUFFER,ve.__webglColorRenderbuffer[Re]);let J=i.get(v[Re]).__webglTexture;t.bindFramebuffer(n.FRAMEBUFFER,ve.__webglFramebuffer),n.framebufferTexture2D(n.DRAW_FRAMEBUFFER,n.COLOR_ATTACHMENT0+Re,n.TEXTURE_2D,J,0)}t.bindFramebuffer(n.DRAW_FRAMEBUFFER,ve.__webglMultisampledFramebuffer)}else if(b.depthBuffer&&b.resolveDepthBuffer===!1&&c){let v=b.stencilBuffer?n.DEPTH_STENCIL_ATTACHMENT:n.DEPTH_ATTACHMENT;n.invalidateFramebuffer(n.DRAW_FRAMEBUFFER,[v])}}}function w(b){return Math.min(r.maxSamples,b.samples)}function Vt(b){let v=i.get(b);return b.samples>0&&e.has("WEBGL_multisampled_render_to_texture")===!0&&v.__useRenderToTexture!==!1}function lt(b){let v=o.render.frame;u.get(b)!==v&&(u.set(b,v),b.update())}function bt(b,v){let N=b.colorSpace,$=b.format,Y=b.type;return b.isCompressedTexture===!0||b.isVideoTexture===!0||N!==Es&&N!==or&&(rt.getTransfer(N)===dt?($!==Jn||Y!==Un)&&Le("WebGLTextures: sRGB encoded textures have to use RGBAFormat and UnsignedByteType."):Ne("WebGLTextures: Unsupported texture color space:",N)),v}function be(b){return typeof HTMLImageElement<"u"&&b instanceof HTMLImageElement?(l.width=b.naturalWidth||b.width,l.height=b.naturalHeight||b.height):typeof VideoFrame<"u"&&b instanceof VideoFrame?(l.width=b.displayWidth,l.height=b.displayHeight):(l.width=b.width,l.height=b.height),l}this.allocateTextureUnit=V,this.resetTextureUnits=U,this.setTexture2D=B,this.setTexture2DArray=H,this.setTexture3D=L,this.setTextureCube=Q,this.rebindTextures=Yt,this.setupRenderTarget=st,this.updateRenderTargetMipmap=ft,this.updateMultisampleRenderTarget=Ft,this.setupDepthRenderbuffer=Fe,this.setupFrameBufferTexture=se,this.useMultisampledRTT=Vt,this.isReversedDepthBuffer=function(){return t.buffers.depth.getReversed()}}function UR(n,e){function t(i,r=or){let s,o=rt.getTransfer(r);if(i===Un)return n.UNSIGNED_BYTE;if(i===ld)return n.UNSIGNED_SHORT_4_4_4_4;if(i===ud)return n.UNSIGNED_SHORT_5_5_5_1;if(i===lg)return n.UNSIGNED_INT_5_9_9_9_REV;if(i===ug)return n.UNSIGNED_INT_10F_11F_11F_REV;if(i===ag)return n.BYTE;if(i===cg)return n.SHORT;if(i===No)return n.UNSIGNED_SHORT;if(i===cd)return n.INT;if(i===mi)return n.UNSIGNED_INT;if(i===gi)return n.FLOAT;if(i===Ni)return n.HALF_FLOAT;if(i===dg)return n.ALPHA;if(i===fg)return n.RGB;if(i===Jn)return n.RGBA;if(i===wi)return n.DEPTH_COMPONENT;if(i===Hr)return n.DEPTH_STENCIL;if(i===hg)return n.RED;if(i===dd)return n.RED_INTEGER;if(i===ws)return n.RG;if(i===fd)return n.RG_INTEGER;if(i===hd)return n.RGBA_INTEGER;if(i===ac||i===cc||i===lc||i===uc)if(o===dt)if(s=e.get("WEBGL_compressed_texture_s3tc_srgb"),s!==null){if(i===ac)return s.COMPRESSED_SRGB_S3TC_DXT1_EXT;if(i===cc)return s.COMPRESSED_SRGB_ALPHA_S3TC_DXT1_EXT;if(i===lc)return s.COMPRESSED_SRGB_ALPHA_S3TC_DXT3_EXT;if(i===uc)return s.COMPRESSED_SRGB_ALPHA_S3TC_DXT5_EXT}else return null;else if(s=e.get("WEBGL_compressed_texture_s3tc"),s!==null){if(i===ac)return s.COMPRESSED_RGB_S3TC_DXT1_EXT;if(i===cc)return s.COMPRESSED_RGBA_S3TC_DXT1_EXT;if(i===lc)return s.COMPRESSED_RGBA_S3TC_DXT3_EXT;if(i===uc)return s.COMPRESSED_RGBA_S3TC_DXT5_EXT}else return null;if(i===pd||i===md||i===gd||i===vd)if(s=e.get("WEBGL_compressed_texture_pvrtc"),s!==null){if(i===pd)return s.COMPRESSED_RGB_PVRTC_4BPPV1_IMG;if(i===md)return s.COMPRESSED_RGB_PVRTC_2BPPV1_IMG;if(i===gd)return s.COMPRESSED_RGBA_PVRTC_4BPPV1_IMG;if(i===vd)return s.COMPRESSED_RGBA_PVRTC_2BPPV1_IMG}else return null;if(i===yd||i===_d||i===xd||i===Md||i===bd||i===Ed||i===Sd)if(s=e.get("WEBGL_compressed_texture_etc"),s!==null){if(i===yd||i===_d)return o===dt?s.COMPRESSED_SRGB8_ETC2:s.COMPRESSED_RGB8_ETC2;if(i===xd)return o===dt?s.COMPRESSED_SRGB8_ALPHA8_ETC2_EAC:s.COMPRESSED_RGBA8_ETC2_EAC;if(i===Md)return s.COMPRESSED_R11_EAC;if(i===bd)return s.COMPRESSED_SIGNED_R11_EAC;if(i===Ed)return s.COMPRESSED_RG11_EAC;if(i===Sd)return s.COMPRESSED_SIGNED_RG11_EAC}else return null;if(i===Td||i===Cd||i===wd||i===Dd||i===Id||i===Ad||i===Rd||i===Nd||i===Pd||i===Ld||i===Fd||i===Od||i===kd||i===Ud)if(s=e.get("WEBGL_compressed_texture_astc"),s!==null){if(i===Td)return o===dt?s.COMPRESSED_SRGB8_ALPHA8_ASTC_4x4_KHR:s.COMPRESSED_RGBA_ASTC_4x4_KHR;if(i===Cd)return o===dt?s.COMPRESSED_SRGB8_ALPHA8_ASTC_5x4_KHR:s.COMPRESSED_RGBA_ASTC_5x4_KHR;if(i===wd)return o===dt?s.COMPRESSED_SRGB8_ALPHA8_ASTC_5x5_KHR:s.COMPRESSED_RGBA_ASTC_5x5_KHR;if(i===Dd)return o===dt?s.COMPRESSED_SRGB8_ALPHA8_ASTC_6x5_KHR:s.COMPRESSED_RGBA_ASTC_6x5_KHR;if(i===Id)return o===dt?s.COMPRESSED_SRGB8_ALPHA8_ASTC_6x6_KHR:s.COMPRESSED_RGBA_ASTC_6x6_KHR;if(i===Ad)return o===dt?s.COMPRESSED_SRGB8_ALPHA8_ASTC_8x5_KHR:s.COMPRESSED_RGBA_ASTC_8x5_KHR;if(i===Rd)return o===dt?s.COMPRESSED_SRGB8_ALPHA8_ASTC_8x6_KHR:s.COMPRESSED_RGBA_ASTC_8x6_KHR;if(i===Nd)return o===dt?s.COMPRESSED_SRGB8_ALPHA8_ASTC_8x8_KHR:s.COMPRESSED_RGBA_ASTC_8x8_KHR;if(i===Pd)return o===dt?s.COMPRESSED_SRGB8_ALPHA8_ASTC_10x5_KHR:s.COMPRESSED_RGBA_ASTC_10x5_KHR;if(i===Ld)return o===dt?s.COMPRESSED_SRGB8_ALPHA8_ASTC_10x6_KHR:s.COMPRESSED_RGBA_ASTC_10x6_KHR;if(i===Fd)return o===dt?s.COMPRESSED_SRGB8_ALPHA8_ASTC_10x8_KHR:s.COMPRESSED_RGBA_ASTC_10x8_KHR;if(i===Od)return o===dt?s.COMPRESSED_SRGB8_ALPHA8_ASTC_10x10_KHR:s.COMPRESSED_RGBA_ASTC_10x10_KHR;if(i===kd)return o===dt?s.COMPRESSED_SRGB8_ALPHA8_ASTC_12x10_KHR:s.COMPRESSED_RGBA_ASTC_12x10_KHR;if(i===Ud)return o===dt?s.COMPRESSED_SRGB8_ALPHA8_ASTC_12x12_KHR:s.COMPRESSED_RGBA_ASTC_12x12_KHR}else return null;if(i===Bd||i===Vd||i===Hd)if(s=e.get("EXT_texture_compression_bptc"),s!==null){if(i===Bd)return o===dt?s.COMPRESSED_SRGB_ALPHA_BPTC_UNORM_EXT:s.COMPRESSED_RGBA_BPTC_UNORM_EXT;if(i===Vd)return s.COMPRESSED_RGB_BPTC_SIGNED_FLOAT_EXT;if(i===Hd)return s.COMPRESSED_RGB_BPTC_UNSIGNED_FLOAT_EXT}else return null;if(i===zd||i===Gd||i===Wd||i===jd)if(s=e.get("EXT_texture_compression_rgtc"),s!==null){if(i===zd)return s.COMPRESSED_RED_RGTC1_EXT;if(i===Gd)return s.COMPRESSED_SIGNED_RED_RGTC1_EXT;if(i===Wd)return s.COMPRESSED_RED_GREEN_RGTC2_EXT;if(i===jd)return s.COMPRESSED_SIGNED_RED_GREEN_RGTC2_EXT}else return null;return i===Po?n.UNSIGNED_INT_24_8:n[i]!==void 0?n[i]:null}return{convert:t}}var BR=`
void main() {

	gl_Position = vec4( position, 1.0 );

}`,VR=`
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

}`,Pg=class{constructor(){this.texture=null,this.mesh=null,this.depthNear=0,this.depthFar=0}init(e,t){if(this.texture===null){let i=new Ka(e.texture);(e.depthNear!==t.depthNear||e.depthFar!==t.depthFar)&&(this.depthNear=e.depthNear,this.depthFar=e.depthFar),this.texture=i}}getMesh(e){if(this.texture!==null&&this.mesh===null){let t=e.cameras[0].viewport,i=new On({vertexShader:BR,fragmentShader:VR,uniforms:{depthColor:{value:this.texture},depthWidth:{value:t.z},depthHeight:{value:t.w}}});this.mesh=new Zn(new Qa(20,20),i)}return this.mesh}reset(){this.texture=null,this.mesh=null}getDepthTexture(){return this.texture}},Lg=class extends sr{constructor(e,t){super();let i=this,r=null,s=1,o=null,a="local-floor",c=1,l=null,u=null,f=null,d=null,h=null,g=null,x=typeof XRWebGLBinding<"u",m=new Pg,p={},M=t.getContextAttributes(),T=null,S=null,I=[],D=[],A=new pt,y=null,E=new hn;E.viewport=new Lt;let q=new hn;q.viewport=new Lt;let C=[E,q],U=new rd,V=null,W=null;this.cameraAutoUpdate=!0,this.enabled=!1,this.isPresenting=!1,this.getController=function(X){let ne=I[X];return ne===void 0&&(ne=new Do,I[X]=ne),ne.getTargetRaySpace()},this.getControllerGrip=function(X){let ne=I[X];return ne===void 0&&(ne=new Do,I[X]=ne),ne.getGripSpace()},this.getHand=function(X){let ne=I[X];return ne===void 0&&(ne=new Do,I[X]=ne),ne.getHandSpace()};function B(X){let ne=D.indexOf(X.inputSource);if(ne===-1)return;let se=I[ne];se!==void 0&&(se.update(X.inputSource,X.frame,l||o),se.dispatchEvent({type:X.type,data:X.inputSource}))}function H(){r.removeEventListener("select",B),r.removeEventListener("selectstart",B),r.removeEventListener("selectend",B),r.removeEventListener("squeeze",B),r.removeEventListener("squeezestart",B),r.removeEventListener("squeezeend",B),r.removeEventListener("end",H),r.removeEventListener("inputsourceschange",L);for(let X=0;X<I.length;X++){let ne=D[X];ne!==null&&(D[X]=null,I[X].disconnect(ne))}V=null,W=null,m.reset();for(let X in p)delete p[X];e.setRenderTarget(T),h=null,d=null,f=null,r=null,S=null,St.stop(),i.isPresenting=!1,e.setPixelRatio(y),e.setSize(A.width,A.height,!1),i.dispatchEvent({type:"sessionend"})}this.setFramebufferScaleFactor=function(X){s=X,i.isPresenting===!0&&Le("WebXRManager: Cannot change framebuffer scale while presenting.")},this.setReferenceSpaceType=function(X){a=X,i.isPresenting===!0&&Le("WebXRManager: Cannot change reference space type while presenting.")},this.getReferenceSpace=function(){return l||o},this.setReferenceSpace=function(X){l=X},this.getBaseLayer=function(){return d!==null?d:h},this.getBinding=function(){return f===null&&x&&(f=new XRWebGLBinding(r,t)),f},this.getFrame=function(){return g},this.getSession=function(){return r},this.setSession=async function(X){if(r=X,r!==null){if(T=e.getRenderTarget(),r.addEventListener("select",B),r.addEventListener("selectstart",B),r.addEventListener("selectend",B),r.addEventListener("squeeze",B),r.addEventListener("squeezestart",B),r.addEventListener("squeezeend",B),r.addEventListener("end",H),r.addEventListener("inputsourceschange",L),M.xrCompatible!==!0&&await t.makeXRCompatible(),y=e.getPixelRatio(),e.getSize(A),x&&"createProjectionLayer"in XRWebGLBinding.prototype){let se=null,Ve=null,Ie=null;M.depth&&(Ie=M.stencil?t.DEPTH24_STENCIL8:t.DEPTH_COMPONENT24,se=M.stencil?Hr:wi,Ve=M.stencil?Po:mi);let Fe={colorFormat:t.RGBA8,depthFormat:Ie,scaleFactor:s};f=this.getBinding(),d=f.createProjectionLayer(Fe),r.updateRenderState({layers:[d]}),e.setPixelRatio(1),e.setSize(d.textureWidth,d.textureHeight,!1),S=new Ln(d.textureWidth,d.textureHeight,{format:Jn,type:Un,depthTexture:new Fr(d.textureWidth,d.textureHeight,Ve,void 0,void 0,void 0,void 0,void 0,void 0,se),stencilBuffer:M.stencil,colorSpace:e.outputColorSpace,samples:M.antialias?4:0,resolveDepthBuffer:d.ignoreDepthValues===!1,resolveStencilBuffer:d.ignoreDepthValues===!1})}else{let se={antialias:M.antialias,alpha:!0,depth:M.depth,stencil:M.stencil,framebufferScaleFactor:s};h=new XRWebGLLayer(r,t,se),r.updateRenderState({baseLayer:h}),e.setPixelRatio(1),e.setSize(h.framebufferWidth,h.framebufferHeight,!1),S=new Ln(h.framebufferWidth,h.framebufferHeight,{format:Jn,type:Un,colorSpace:e.outputColorSpace,stencilBuffer:M.stencil,resolveDepthBuffer:h.ignoreDepthValues===!1,resolveStencilBuffer:h.ignoreDepthValues===!1})}S.isXRRenderTarget=!0,this.setFoveation(c),l=null,o=await r.requestReferenceSpace(a),St.setContext(r),St.start(),i.isPresenting=!0,i.dispatchEvent({type:"sessionstart"})}},this.getEnvironmentBlendMode=function(){if(r!==null)return r.environmentBlendMode},this.getDepthTexture=function(){return m.getDepthTexture()};function L(X){for(let ne=0;ne<X.removed.length;ne++){let se=X.removed[ne],Ve=D.indexOf(se);Ve>=0&&(D[Ve]=null,I[Ve].disconnect(se))}for(let ne=0;ne<X.added.length;ne++){let se=X.added[ne],Ve=D.indexOf(se);if(Ve===-1){for(let Fe=0;Fe<I.length;Fe++)if(Fe>=D.length){D.push(se),Ve=Fe;break}else if(D[Fe]===null){D[Fe]=se,Ve=Fe;break}if(Ve===-1)break}let Ie=I[Ve];Ie&&Ie.connect(se)}}let Q=new k,Z=new k;function ue(X,ne,se){Q.setFromMatrixPosition(ne.matrixWorld),Z.setFromMatrixPosition(se.matrixWorld);let Ve=Q.distanceTo(Z),Ie=ne.projectionMatrix.elements,Fe=se.projectionMatrix.elements,Yt=Ie[14]/(Ie[10]-1),st=Ie[14]/(Ie[10]+1),ft=(Ie[9]+1)/Ie[5],_t=(Ie[9]-1)/Ie[5],We=(Ie[8]-1)/Ie[0],Ft=(Fe[8]+1)/Fe[0],w=Yt*We,Vt=Yt*Ft,lt=Ve/(-We+Ft),bt=lt*-We;if(ne.matrixWorld.decompose(X.position,X.quaternion,X.scale),X.translateX(bt),X.translateZ(lt),X.matrixWorld.compose(X.position,X.quaternion,X.scale),X.matrixWorldInverse.copy(X.matrixWorld).invert(),Ie[10]===-1)X.projectionMatrix.copy(ne.projectionMatrix),X.projectionMatrixInverse.copy(ne.projectionMatrixInverse);else{let be=Yt+lt,b=st+lt,v=w-bt,N=Vt+(Ve-bt),$=ft*st/b*be,Y=_t*st/b*be;X.projectionMatrix.makePerspective(v,N,$,Y,be,b),X.projectionMatrixInverse.copy(X.projectionMatrix).invert()}}function ge(X,ne){ne===null?X.matrixWorld.copy(X.matrix):X.matrixWorld.multiplyMatrices(ne.matrixWorld,X.matrix),X.matrixWorldInverse.copy(X.matrixWorld).invert()}this.updateCamera=function(X){if(r===null)return;let ne=X.near,se=X.far;m.texture!==null&&(m.depthNear>0&&(ne=m.depthNear),m.depthFar>0&&(se=m.depthFar)),U.near=q.near=E.near=ne,U.far=q.far=E.far=se,(V!==U.near||W!==U.far)&&(r.updateRenderState({depthNear:U.near,depthFar:U.far}),V=U.near,W=U.far),U.layers.mask=X.layers.mask|6,E.layers.mask=U.layers.mask&-5,q.layers.mask=U.layers.mask&-3;let Ve=X.parent,Ie=U.cameras;ge(U,Ve);for(let Fe=0;Fe<Ie.length;Fe++)ge(Ie[Fe],Ve);Ie.length===2?ue(U,E,q):U.projectionMatrix.copy(E.projectionMatrix),fe(X,U,Ve)};function fe(X,ne,se){se===null?X.matrix.copy(ne.matrixWorld):(X.matrix.copy(se.matrixWorld),X.matrix.invert(),X.matrix.multiply(ne.matrixWorld)),X.matrix.decompose(X.position,X.quaternion,X.scale),X.updateMatrixWorld(!0),X.projectionMatrix.copy(ne.projectionMatrix),X.projectionMatrixInverse.copy(ne.projectionMatrixInverse),X.isPerspectiveCamera&&(X.fov=Bu*2*Math.atan(1/X.projectionMatrix.elements[5]),X.zoom=1)}this.getCamera=function(){return U},this.getFoveation=function(){if(!(d===null&&h===null))return c},this.setFoveation=function(X){c=X,d!==null&&(d.fixedFoveation=X),h!==null&&h.fixedFoveation!==void 0&&(h.fixedFoveation=X)},this.hasDepthSensing=function(){return m.texture!==null},this.getDepthSensingMesh=function(){return m.getMesh(U)},this.getCameraTexture=function(X){return p[X]};let ze=null;function Tt(X,ne){if(u=ne.getViewerPose(l||o),g=ne,u!==null){let se=u.views;h!==null&&(e.setRenderTargetFramebuffer(S,h.framebuffer),e.setRenderTarget(S));let Ve=!1;se.length!==U.cameras.length&&(U.cameras.length=0,Ve=!0);for(let st=0;st<se.length;st++){let ft=se[st],_t=null;if(h!==null)_t=h.getViewport(ft);else{let Ft=f.getViewSubImage(d,ft);_t=Ft.viewport,st===0&&(e.setRenderTargetTextures(S,Ft.colorTexture,Ft.depthStencilTexture),e.setRenderTarget(S))}let We=C[st];We===void 0&&(We=new hn,We.layers.enable(st),We.viewport=new Lt,C[st]=We),We.matrix.fromArray(ft.transform.matrix),We.matrix.decompose(We.position,We.quaternion,We.scale),We.projectionMatrix.fromArray(ft.projectionMatrix),We.projectionMatrixInverse.copy(We.projectionMatrix).invert(),We.viewport.set(_t.x,_t.y,_t.width,_t.height),st===0&&(U.matrix.copy(We.matrix),U.matrix.decompose(U.position,U.quaternion,U.scale)),Ve===!0&&U.cameras.push(We)}let Ie=r.enabledFeatures;if(Ie&&Ie.includes("depth-sensing")&&r.depthUsage=="gpu-optimized"&&x){f=i.getBinding();let st=f.getDepthInformation(se[0]);st&&st.isValid&&st.texture&&m.init(st,r.renderState)}if(Ie&&Ie.includes("camera-access")&&x){e.state.unbindTexture(),f=i.getBinding();for(let st=0;st<se.length;st++){let ft=se[st].camera;if(ft){let _t=p[ft];_t||(_t=new Ka,p[ft]=_t);let We=f.getCameraImage(ft);_t.sourceTexture=We}}}}for(let se=0;se<I.length;se++){let Ve=D[se],Ie=I[se];Ve!==null&&Ie!==void 0&&Ie.update(Ve,ne,l||o)}ze&&ze(X,ne),ne.detectedPlanes&&i.dispatchEvent({type:"planesdetected",data:ne}),g=null}let St=new EM;St.setAnimationLoop(Tt),this.setAnimationLoop=function(X){ze=X},this.dispose=function(){}}},As=new Ss,HR=new Bt;function zR(n,e){function t(m,p){m.matrixAutoUpdate===!0&&m.updateMatrix(),p.value.copy(m.matrix)}function i(m,p){p.color.getRGB(m.fogColor.value,gg(n)),p.isFog?(m.fogNear.value=p.near,m.fogFar.value=p.far):p.isFogExp2&&(m.fogDensity.value=p.density)}function r(m,p,M,T,S){p.isMeshBasicMaterial?s(m,p):p.isMeshLambertMaterial?(s(m,p),p.envMap&&(m.envMapIntensity.value=p.envMapIntensity)):p.isMeshToonMaterial?(s(m,p),f(m,p)):p.isMeshPhongMaterial?(s(m,p),u(m,p),p.envMap&&(m.envMapIntensity.value=p.envMapIntensity)):p.isMeshStandardMaterial?(s(m,p),d(m,p),p.isMeshPhysicalMaterial&&h(m,p,S)):p.isMeshMatcapMaterial?(s(m,p),g(m,p)):p.isMeshDepthMaterial?s(m,p):p.isMeshDistanceMaterial?(s(m,p),x(m,p)):p.isMeshNormalMaterial?s(m,p):p.isLineBasicMaterial?(o(m,p),p.isLineDashedMaterial&&a(m,p)):p.isPointsMaterial?c(m,p,M,T):p.isSpriteMaterial?l(m,p):p.isShadowMaterial?(m.color.value.copy(p.color),m.opacity.value=p.opacity):p.isShaderMaterial&&(p.uniformsNeedUpdate=!1)}function s(m,p){m.opacity.value=p.opacity,p.color&&m.diffuse.value.copy(p.color),p.emissive&&m.emissive.value.copy(p.emissive).multiplyScalar(p.emissiveIntensity),p.map&&(m.map.value=p.map,t(p.map,m.mapTransform)),p.alphaMap&&(m.alphaMap.value=p.alphaMap,t(p.alphaMap,m.alphaMapTransform)),p.bumpMap&&(m.bumpMap.value=p.bumpMap,t(p.bumpMap,m.bumpMapTransform),m.bumpScale.value=p.bumpScale,p.side===xn&&(m.bumpScale.value*=-1)),p.normalMap&&(m.normalMap.value=p.normalMap,t(p.normalMap,m.normalMapTransform),m.normalScale.value.copy(p.normalScale),p.side===xn&&m.normalScale.value.negate()),p.displacementMap&&(m.displacementMap.value=p.displacementMap,t(p.displacementMap,m.displacementMapTransform),m.displacementScale.value=p.displacementScale,m.displacementBias.value=p.displacementBias),p.emissiveMap&&(m.emissiveMap.value=p.emissiveMap,t(p.emissiveMap,m.emissiveMapTransform)),p.specularMap&&(m.specularMap.value=p.specularMap,t(p.specularMap,m.specularMapTransform)),p.alphaTest>0&&(m.alphaTest.value=p.alphaTest);let M=e.get(p),T=M.envMap,S=M.envMapRotation;T&&(m.envMap.value=T,As.copy(S),As.x*=-1,As.y*=-1,As.z*=-1,T.isCubeTexture&&T.isRenderTargetTexture===!1&&(As.y*=-1,As.z*=-1),m.envMapRotation.value.setFromMatrix4(HR.makeRotationFromEuler(As)),m.flipEnvMap.value=T.isCubeTexture&&T.isRenderTargetTexture===!1?-1:1,m.reflectivity.value=p.reflectivity,m.ior.value=p.ior,m.refractionRatio.value=p.refractionRatio),p.lightMap&&(m.lightMap.value=p.lightMap,m.lightMapIntensity.value=p.lightMapIntensity,t(p.lightMap,m.lightMapTransform)),p.aoMap&&(m.aoMap.value=p.aoMap,m.aoMapIntensity.value=p.aoMapIntensity,t(p.aoMap,m.aoMapTransform))}function o(m,p){m.diffuse.value.copy(p.color),m.opacity.value=p.opacity,p.map&&(m.map.value=p.map,t(p.map,m.mapTransform))}function a(m,p){m.dashSize.value=p.dashSize,m.totalSize.value=p.dashSize+p.gapSize,m.scale.value=p.scale}function c(m,p,M,T){m.diffuse.value.copy(p.color),m.opacity.value=p.opacity,m.size.value=p.size*M,m.scale.value=T*.5,p.map&&(m.map.value=p.map,t(p.map,m.uvTransform)),p.alphaMap&&(m.alphaMap.value=p.alphaMap,t(p.alphaMap,m.alphaMapTransform)),p.alphaTest>0&&(m.alphaTest.value=p.alphaTest)}function l(m,p){m.diffuse.value.copy(p.color),m.opacity.value=p.opacity,m.rotation.value=p.rotation,p.map&&(m.map.value=p.map,t(p.map,m.mapTransform)),p.alphaMap&&(m.alphaMap.value=p.alphaMap,t(p.alphaMap,m.alphaMapTransform)),p.alphaTest>0&&(m.alphaTest.value=p.alphaTest)}function u(m,p){m.specular.value.copy(p.specular),m.shininess.value=Math.max(p.shininess,1e-4)}function f(m,p){p.gradientMap&&(m.gradientMap.value=p.gradientMap)}function d(m,p){m.metalness.value=p.metalness,p.metalnessMap&&(m.metalnessMap.value=p.metalnessMap,t(p.metalnessMap,m.metalnessMapTransform)),m.roughness.value=p.roughness,p.roughnessMap&&(m.roughnessMap.value=p.roughnessMap,t(p.roughnessMap,m.roughnessMapTransform)),p.envMap&&(m.envMapIntensity.value=p.envMapIntensity)}function h(m,p,M){m.ior.value=p.ior,p.sheen>0&&(m.sheenColor.value.copy(p.sheenColor).multiplyScalar(p.sheen),m.sheenRoughness.value=p.sheenRoughness,p.sheenColorMap&&(m.sheenColorMap.value=p.sheenColorMap,t(p.sheenColorMap,m.sheenColorMapTransform)),p.sheenRoughnessMap&&(m.sheenRoughnessMap.value=p.sheenRoughnessMap,t(p.sheenRoughnessMap,m.sheenRoughnessMapTransform))),p.clearcoat>0&&(m.clearcoat.value=p.clearcoat,m.clearcoatRoughness.value=p.clearcoatRoughness,p.clearcoatMap&&(m.clearcoatMap.value=p.clearcoatMap,t(p.clearcoatMap,m.clearcoatMapTransform)),p.clearcoatRoughnessMap&&(m.clearcoatRoughnessMap.value=p.clearcoatRoughnessMap,t(p.clearcoatRoughnessMap,m.clearcoatRoughnessMapTransform)),p.clearcoatNormalMap&&(m.clearcoatNormalMap.value=p.clearcoatNormalMap,t(p.clearcoatNormalMap,m.clearcoatNormalMapTransform),m.clearcoatNormalScale.value.copy(p.clearcoatNormalScale),p.side===xn&&m.clearcoatNormalScale.value.negate())),p.dispersion>0&&(m.dispersion.value=p.dispersion),p.iridescence>0&&(m.iridescence.value=p.iridescence,m.iridescenceIOR.value=p.iridescenceIOR,m.iridescenceThicknessMinimum.value=p.iridescenceThicknessRange[0],m.iridescenceThicknessMaximum.value=p.iridescenceThicknessRange[1],p.iridescenceMap&&(m.iridescenceMap.value=p.iridescenceMap,t(p.iridescenceMap,m.iridescenceMapTransform)),p.iridescenceThicknessMap&&(m.iridescenceThicknessMap.value=p.iridescenceThicknessMap,t(p.iridescenceThicknessMap,m.iridescenceThicknessMapTransform))),p.transmission>0&&(m.transmission.value=p.transmission,m.transmissionSamplerMap.value=M.texture,m.transmissionSamplerSize.value.set(M.width,M.height),p.transmissionMap&&(m.transmissionMap.value=p.transmissionMap,t(p.transmissionMap,m.transmissionMapTransform)),m.thickness.value=p.thickness,p.thicknessMap&&(m.thicknessMap.value=p.thicknessMap,t(p.thicknessMap,m.thicknessMapTransform)),m.attenuationDistance.value=p.attenuationDistance,m.attenuationColor.value.copy(p.attenuationColor)),p.anisotropy>0&&(m.anisotropyVector.value.set(p.anisotropy*Math.cos(p.anisotropyRotation),p.anisotropy*Math.sin(p.anisotropyRotation)),p.anisotropyMap&&(m.anisotropyMap.value=p.anisotropyMap,t(p.anisotropyMap,m.anisotropyMapTransform))),m.specularIntensity.value=p.specularIntensity,m.specularColor.value.copy(p.specularColor),p.specularColorMap&&(m.specularColorMap.value=p.specularColorMap,t(p.specularColorMap,m.specularColorMapTransform)),p.specularIntensityMap&&(m.specularIntensityMap.value=p.specularIntensityMap,t(p.specularIntensityMap,m.specularIntensityMapTransform))}function g(m,p){p.matcap&&(m.matcap.value=p.matcap)}function x(m,p){let M=e.get(p).light;m.referencePosition.value.setFromMatrixPosition(M.matrixWorld),m.nearDistance.value=M.shadow.camera.near,m.farDistance.value=M.shadow.camera.far}return{refreshFogUniforms:i,refreshMaterialUniforms:r}}function GR(n,e,t,i){let r={},s={},o=[],a=n.getParameter(n.MAX_UNIFORM_BUFFER_BINDINGS);function c(M,T){let S=T.program;i.uniformBlockBinding(M,S)}function l(M,T){let S=r[M.id];S===void 0&&(g(M),S=u(M),r[M.id]=S,M.addEventListener("dispose",m));let I=T.program;i.updateUBOMapping(M,I);let D=e.render.frame;s[M.id]!==D&&(d(M),s[M.id]=D)}function u(M){let T=f();M.__bindingPointIndex=T;let S=n.createBuffer(),I=M.__size,D=M.usage;return n.bindBuffer(n.UNIFORM_BUFFER,S),n.bufferData(n.UNIFORM_BUFFER,I,D),n.bindBuffer(n.UNIFORM_BUFFER,null),n.bindBufferBase(n.UNIFORM_BUFFER,T,S),S}function f(){for(let M=0;M<a;M++)if(o.indexOf(M)===-1)return o.push(M),M;return Ne("WebGLRenderer: Maximum number of simultaneously usable uniforms groups reached."),0}function d(M){let T=r[M.id],S=M.uniforms,I=M.__cache;n.bindBuffer(n.UNIFORM_BUFFER,T);for(let D=0,A=S.length;D<A;D++){let y=Array.isArray(S[D])?S[D]:[S[D]];for(let E=0,q=y.length;E<q;E++){let C=y[E];if(h(C,D,E,I)===!0){let U=C.__offset,V=Array.isArray(C.value)?C.value:[C.value],W=0;for(let B=0;B<V.length;B++){let H=V[B],L=x(H);typeof H=="number"||typeof H=="boolean"?(C.__data[0]=H,n.bufferSubData(n.UNIFORM_BUFFER,U+W,C.__data)):H.isMatrix3?(C.__data[0]=H.elements[0],C.__data[1]=H.elements[1],C.__data[2]=H.elements[2],C.__data[3]=0,C.__data[4]=H.elements[3],C.__data[5]=H.elements[4],C.__data[6]=H.elements[5],C.__data[7]=0,C.__data[8]=H.elements[6],C.__data[9]=H.elements[7],C.__data[10]=H.elements[8],C.__data[11]=0):(H.toArray(C.__data,W),W+=L.storage/Float32Array.BYTES_PER_ELEMENT)}n.bufferSubData(n.UNIFORM_BUFFER,U,C.__data)}}}n.bindBuffer(n.UNIFORM_BUFFER,null)}function h(M,T,S,I){let D=M.value,A=T+"_"+S;if(I[A]===void 0)return typeof D=="number"||typeof D=="boolean"?I[A]=D:I[A]=D.clone(),!0;{let y=I[A];if(typeof D=="number"||typeof D=="boolean"){if(y!==D)return I[A]=D,!0}else if(y.equals(D)===!1)return y.copy(D),!0}return!1}function g(M){let T=M.uniforms,S=0,I=16;for(let A=0,y=T.length;A<y;A++){let E=Array.isArray(T[A])?T[A]:[T[A]];for(let q=0,C=E.length;q<C;q++){let U=E[q],V=Array.isArray(U.value)?U.value:[U.value];for(let W=0,B=V.length;W<B;W++){let H=V[W],L=x(H),Q=S%I,Z=Q%L.boundary,ue=Q+Z;S+=Z,ue!==0&&I-ue<L.storage&&(S+=I-ue),U.__data=new Float32Array(L.storage/Float32Array.BYTES_PER_ELEMENT),U.__offset=S,S+=L.storage}}}let D=S%I;return D>0&&(S+=I-D),M.__size=S,M.__cache={},this}function x(M){let T={boundary:0,storage:0};return typeof M=="number"||typeof M=="boolean"?(T.boundary=4,T.storage=4):M.isVector2?(T.boundary=8,T.storage=8):M.isVector3||M.isColor?(T.boundary=16,T.storage=12):M.isVector4?(T.boundary=16,T.storage=16):M.isMatrix3?(T.boundary=48,T.storage=48):M.isMatrix4?(T.boundary=64,T.storage=64):M.isTexture?Le("WebGLRenderer: Texture samplers can not be part of an uniforms group."):Le("WebGLRenderer: Unsupported uniform value type.",M),T}function m(M){let T=M.target;T.removeEventListener("dispose",m);let S=o.indexOf(T.__bindingPointIndex);o.splice(S,1),n.deleteBuffer(r[T.id]),delete r[T.id],delete s[T.id]}function p(){for(let M in r)n.deleteBuffer(r[M]);o=[],r={},s={}}return{bind:c,update:l,dispose:p}}var WR=new Uint16Array([12469,15057,12620,14925,13266,14620,13807,14376,14323,13990,14545,13625,14713,13328,14840,12882,14931,12528,14996,12233,15039,11829,15066,11525,15080,11295,15085,10976,15082,10705,15073,10495,13880,14564,13898,14542,13977,14430,14158,14124,14393,13732,14556,13410,14702,12996,14814,12596,14891,12291,14937,11834,14957,11489,14958,11194,14943,10803,14921,10506,14893,10278,14858,9960,14484,14039,14487,14025,14499,13941,14524,13740,14574,13468,14654,13106,14743,12678,14818,12344,14867,11893,14889,11509,14893,11180,14881,10751,14852,10428,14812,10128,14765,9754,14712,9466,14764,13480,14764,13475,14766,13440,14766,13347,14769,13070,14786,12713,14816,12387,14844,11957,14860,11549,14868,11215,14855,10751,14825,10403,14782,10044,14729,9651,14666,9352,14599,9029,14967,12835,14966,12831,14963,12804,14954,12723,14936,12564,14917,12347,14900,11958,14886,11569,14878,11247,14859,10765,14828,10401,14784,10011,14727,9600,14660,9289,14586,8893,14508,8533,15111,12234,15110,12234,15104,12216,15092,12156,15067,12010,15028,11776,14981,11500,14942,11205,14902,10752,14861,10393,14812,9991,14752,9570,14682,9252,14603,8808,14519,8445,14431,8145,15209,11449,15208,11451,15202,11451,15190,11438,15163,11384,15117,11274,15055,10979,14994,10648,14932,10343,14871,9936,14803,9532,14729,9218,14645,8742,14556,8381,14461,8020,14365,7603,15273,10603,15272,10607,15267,10619,15256,10631,15231,10614,15182,10535,15118,10389,15042,10167,14963,9787,14883,9447,14800,9115,14710,8665,14615,8318,14514,7911,14411,7507,14279,7198,15314,9675,15313,9683,15309,9712,15298,9759,15277,9797,15229,9773,15166,9668,15084,9487,14995,9274,14898,8910,14800,8539,14697,8234,14590,7790,14479,7409,14367,7067,14178,6621,15337,8619,15337,8631,15333,8677,15325,8769,15305,8871,15264,8940,15202,8909,15119,8775,15022,8565,14916,8328,14804,8009,14688,7614,14569,7287,14448,6888,14321,6483,14088,6171,15350,7402,15350,7419,15347,7480,15340,7613,15322,7804,15287,7973,15229,8057,15148,8012,15046,7846,14933,7611,14810,7357,14682,7069,14552,6656,14421,6316,14251,5948,14007,5528,15356,5942,15356,5977,15353,6119,15348,6294,15332,6551,15302,6824,15249,7044,15171,7122,15070,7050,14949,6861,14818,6611,14679,6349,14538,6067,14398,5651,14189,5311,13935,4958,15359,4123,15359,4153,15356,4296,15353,4646,15338,5160,15311,5508,15263,5829,15188,6042,15088,6094,14966,6001,14826,5796,14678,5543,14527,5287,14377,4985,14133,4586,13869,4257,15360,1563,15360,1642,15358,2076,15354,2636,15341,3350,15317,4019,15273,4429,15203,4732,15105,4911,14981,4932,14836,4818,14679,4621,14517,4386,14359,4156,14083,3795,13808,3437,15360,122,15360,137,15358,285,15355,636,15344,1274,15322,2177,15281,2765,15215,3223,15120,3451,14995,3569,14846,3567,14681,3466,14511,3305,14344,3121,14037,2800,13753,2467,15360,0,15360,1,15359,21,15355,89,15346,253,15325,479,15287,796,15225,1148,15133,1492,15008,1749,14856,1882,14685,1886,14506,1783,14324,1608,13996,1398,13702,1183]),Pi=null;function jR(){return Pi===null&&(Pi=new Gu(WR,16,16,ws,Ni),Pi.name="DFG_LUT",Pi.minFilter=sn,Pi.magFilter=sn,Pi.wrapS=Ci,Pi.wrapT=Ci,Pi.generateMipmaps=!1,Pi.needsUpdate=!0),Pi}var Kd=class{constructor(e={}){let{canvas:t=Yx(),context:i=null,depth:r=!0,stencil:s=!1,alpha:o=!1,antialias:a=!1,premultipliedAlpha:c=!0,preserveDrawingBuffer:l=!1,powerPreference:u="default",failIfMajorPerformanceCaveat:f=!1,reversedDepthBuffer:d=!1,outputBufferType:h=Un}=e;this.isWebGLRenderer=!0;let g;if(i!==null){if(typeof WebGLRenderingContext<"u"&&i instanceof WebGLRenderingContext)throw new Error("THREE.WebGLRenderer: WebGL 1 is not supported since r163.");g=i.getContextAttributes().alpha}else g=o;let x=h,m=new Set([hd,fd,dd]),p=new Set([Un,mi,No,Po,ld,ud]),M=new Uint32Array(4),T=new Int32Array(4),S=null,I=null,D=[],A=[],y=null;this.domElement=t,this.debug={checkShaderErrors:!0,onShaderError:null},this.autoClear=!0,this.autoClearColor=!0,this.autoClearDepth=!0,this.autoClearStencil=!0,this.sortObjects=!0,this.clippingPlanes=[],this.localClippingEnabled=!1,this.toneMapping=pi,this.toneMappingExposure=1,this.transmissionResolutionScale=1;let E=this,q=!1;this._outputColorSpace=Pn;let C=0,U=0,V=null,W=-1,B=null,H=new Lt,L=new Lt,Q=null,Z=new Xe(0),ue=0,ge=t.width,fe=t.height,ze=1,Tt=null,St=null,X=new Lt(0,0,ge,fe),ne=new Lt(0,0,ge,fe),se=!1,Ve=new Ya,Ie=!1,Fe=!1,Yt=new Bt,st=new k,ft=new Lt,_t={background:null,fog:null,environment:null,overrideMaterial:null,isScene:!0},We=!1;function Ft(){return V===null?ze:1}let w=i;function Vt(_,P){return t.getContext(_,P)}try{let _={alpha:!0,depth:r,stencil:s,antialias:a,premultipliedAlpha:c,preserveDrawingBuffer:l,powerPreference:u,failIfMajorPerformanceCaveat:f};if("setAttribute"in t&&t.setAttribute("data-engine",`three.js r${"183"}`),t.addEventListener("webglcontextlost",_e,!1),t.addEventListener("webglcontextrestored",ke,!1),t.addEventListener("webglcontextcreationerror",Et,!1),w===null){let P="webgl2";if(w=Vt(P,_),w===null)throw Vt(P)?new Error("Error creating WebGL context with your selected attributes."):new Error("Error creating WebGL context.")}}catch(_){throw Ne("WebGLRenderer: "+_.message),_}let lt,bt,be,b,v,N,$,Y,j,ve,ie,Ce,Re,J,ee,ye,xe,de,je,R,re,te,me;function K(){lt=new QA(w),lt.init(),re=new UR(w,lt),bt=new jA(w,lt,e,re),be=new OR(w,lt),bt.reversedDepthBuffer&&d&&be.buffers.depth.setReversed(!0),b=new n1(w),v=new bR,N=new kR(w,lt,be,v,bt,re,b),$=new KA(E),Y=new aD(w),te=new GA(w,Y),j=new e1(w,Y,b,te),ve=new r1(w,j,Y,te,b),de=new i1(w,bt,N),ee=new $A(v),ie=new MR(E,$,lt,bt,te,ee),Ce=new zR(E,v),Re=new SR,J=new AR(lt),xe=new zA(E,$,be,ve,g,c),ye=new FR(E,ve,bt),me=new GR(w,b,bt,be),je=new WA(w,lt,b),R=new t1(w,lt,b),b.programs=ie.programs,E.capabilities=bt,E.extensions=lt,E.properties=v,E.renderLists=Re,E.shadowMap=ye,E.state=be,E.info=b}K(),x!==Un&&(y=new o1(x,t.width,t.height,r,s));let G=new Lg(E,w);this.xr=G,this.getContext=function(){return w},this.getContextAttributes=function(){return w.getContextAttributes()},this.forceContextLoss=function(){let _=lt.get("WEBGL_lose_context");_&&_.loseContext()},this.forceContextRestore=function(){let _=lt.get("WEBGL_lose_context");_&&_.restoreContext()},this.getPixelRatio=function(){return ze},this.setPixelRatio=function(_){_!==void 0&&(ze=_,this.setSize(ge,fe,!1))},this.getSize=function(_){return _.set(ge,fe)},this.setSize=function(_,P,z=!0){if(G.isPresenting){Le("WebGLRenderer: Can't change size while VR device is presenting.");return}ge=_,fe=P,t.width=Math.floor(_*ze),t.height=Math.floor(P*ze),z===!0&&(t.style.width=_+"px",t.style.height=P+"px"),y!==null&&y.setSize(t.width,t.height),this.setViewport(0,0,_,P)},this.getDrawingBufferSize=function(_){return _.set(ge*ze,fe*ze).floor()},this.setDrawingBufferSize=function(_,P,z){ge=_,fe=P,ze=z,t.width=Math.floor(_*z),t.height=Math.floor(P*z),this.setViewport(0,0,_,P)},this.setEffects=function(_){if(x===Un){console.error("THREE.WebGLRenderer: setEffects() requires outputBufferType set to HalfFloatType or FloatType.");return}if(_){for(let P=0;P<_.length;P++)if(_[P].isOutputPass===!0){console.warn("THREE.WebGLRenderer: OutputPass is not needed in setEffects(). Tone mapping and color space conversion are applied automatically.");break}}y.setEffects(_||[])},this.getCurrentViewport=function(_){return _.copy(H)},this.getViewport=function(_){return _.copy(X)},this.setViewport=function(_,P,z,O){_.isVector4?X.set(_.x,_.y,_.z,_.w):X.set(_,P,z,O),be.viewport(H.copy(X).multiplyScalar(ze).round())},this.getScissor=function(_){return _.copy(ne)},this.setScissor=function(_,P,z,O){_.isVector4?ne.set(_.x,_.y,_.z,_.w):ne.set(_,P,z,O),be.scissor(L.copy(ne).multiplyScalar(ze).round())},this.getScissorTest=function(){return se},this.setScissorTest=function(_){be.setScissorTest(se=_)},this.setOpaqueSort=function(_){Tt=_},this.setTransparentSort=function(_){St=_},this.getClearColor=function(_){return _.copy(xe.getClearColor())},this.setClearColor=function(){xe.setClearColor(...arguments)},this.getClearAlpha=function(){return xe.getClearAlpha()},this.setClearAlpha=function(){xe.setClearAlpha(...arguments)},this.clear=function(_=!0,P=!0,z=!0){let O=0;if(_){let F=!1;if(V!==null){let ce=V.texture.format;F=m.has(ce)}if(F){let ce=V.texture.type,he=p.has(ce),le=xe.getClearColor(),Me=xe.getClearAlpha(),Se=le.r,Ue=le.g,$e=le.b;he?(M[0]=Se,M[1]=Ue,M[2]=$e,M[3]=Me,w.clearBufferuiv(w.COLOR,0,M)):(T[0]=Se,T[1]=Ue,T[2]=$e,T[3]=Me,w.clearBufferiv(w.COLOR,0,T))}else O|=w.COLOR_BUFFER_BIT}P&&(O|=w.DEPTH_BUFFER_BIT),z&&(O|=w.STENCIL_BUFFER_BIT,this.state.buffers.stencil.setMask(4294967295)),O!==0&&w.clear(O)},this.clearColor=function(){this.clear(!0,!1,!1)},this.clearDepth=function(){this.clear(!1,!0,!1)},this.clearStencil=function(){this.clear(!1,!1,!0)},this.dispose=function(){t.removeEventListener("webglcontextlost",_e,!1),t.removeEventListener("webglcontextrestored",ke,!1),t.removeEventListener("webglcontextcreationerror",Et,!1),xe.dispose(),Re.dispose(),J.dispose(),v.dispose(),$.dispose(),ve.dispose(),te.dispose(),me.dispose(),ie.dispose(),G.dispose(),G.removeEventListener("sessionstart",Og),G.removeEventListener("sessionend",kg),Gr.stop()};function _e(_){_.preventDefault(),mg("WebGLRenderer: Context Lost."),q=!0}function ke(){mg("WebGLRenderer: Context Restored."),q=!1;let _=b.autoReset,P=ye.enabled,z=ye.autoUpdate,O=ye.needsUpdate,F=ye.type;K(),b.autoReset=_,ye.enabled=P,ye.autoUpdate=z,ye.needsUpdate=O,ye.type=F}function Et(_){Ne("WebGLRenderer: A WebGL context could not be created. Reason: ",_.statusMessage)}function ht(_){let P=_.target;P.removeEventListener("dispose",ht),Fi(P)}function Fi(_){Oi(_),v.remove(_)}function Oi(_){let P=v.get(_).programs;P!==void 0&&(P.forEach(function(z){ie.releaseProgram(z)}),_.isShaderMaterial&&ie.releaseShaderCache(_))}this.renderBufferDirect=function(_,P,z,O,F,ce){P===null&&(P=_t);let he=F.isMesh&&F.matrixWorld.determinant()<0,le=IM(_,P,z,O,F);be.setMaterial(O,he);let Me=z.index,Se=1;if(O.wireframe===!0){if(Me=j.getWireframeAttribute(z),Me===void 0)return;Se=2}let Ue=z.drawRange,$e=z.attributes.position,Te=Ue.start*Se,mt=(Ue.start+Ue.count)*Se;ce!==null&&(Te=Math.max(Te,ce.start*Se),mt=Math.min(mt,(ce.start+ce.count)*Se)),Me!==null?(Te=Math.max(Te,0),mt=Math.min(mt,Me.count)):$e!=null&&(Te=Math.max(Te,0),mt=Math.min(mt,$e.count));let Ot=mt-Te;if(Ot<0||Ot===1/0)return;te.setup(F,O,le,z,Me);let Dt,gt=je;if(Me!==null&&(Dt=Y.get(Me),gt=R,gt.setIndex(Dt)),F.isMesh)O.wireframe===!0?(be.setLineWidth(O.wireframeLinewidth*Ft()),gt.setMode(w.LINES)):gt.setMode(w.TRIANGLES);else if(F.isLine){let on=O.linewidth;on===void 0&&(on=1),be.setLineWidth(on*Ft()),F.isLineSegments?gt.setMode(w.LINES):F.isLineLoop?gt.setMode(w.LINE_LOOP):gt.setMode(w.LINE_STRIP)}else F.isPoints?gt.setMode(w.POINTS):F.isSprite&&gt.setMode(w.TRIANGLES);if(F.isBatchedMesh)if(F._multiDrawInstances!==null)Ha("WebGLRenderer: renderMultiDrawInstances has been deprecated and will be removed in r184. Append to renderMultiDraw arguments and use indirection."),gt.renderMultiDrawInstances(F._multiDrawStarts,F._multiDrawCounts,F._multiDrawCount,F._multiDrawInstances);else if(lt.get("WEBGL_multi_draw"))gt.renderMultiDraw(F._multiDrawStarts,F._multiDrawCounts,F._multiDrawCount);else{let on=F._multiDrawStarts,Ee=F._multiDrawCounts,Tn=F._multiDrawCount,ot=Me?Y.get(Me).bytesPerElement:1,Kn=v.get(O).currentProgram.getUniforms();for(let vi=0;vi<Tn;vi++)Kn.setValue(w,"_gl_DrawID",vi),gt.render(on[vi]/ot,Ee[vi])}else if(F.isInstancedMesh)gt.renderInstances(Te,Ot,F.count);else if(z.isInstancedBufferGeometry){let on=z._maxInstanceCount!==void 0?z._maxInstanceCount:1/0,Ee=Math.min(z.instanceCount,on);gt.renderInstances(Te,Ot,Ee)}else gt.render(Te,Ot)};function Fg(_,P,z){_.transparent===!0&&_.side===Ai&&_.forceSinglePass===!1?(_.side=xn,_.needsUpdate=!0,gc(_,P,z),_.side=ir,_.needsUpdate=!0,gc(_,P,z),_.side=Ai):gc(_,P,z)}this.compile=function(_,P,z=null){z===null&&(z=_),I=J.get(z),I.init(P),A.push(I),z.traverseVisible(function(F){F.isLight&&F.layers.test(P.layers)&&(I.pushLight(F),F.castShadow&&I.pushShadow(F))}),_!==z&&_.traverseVisible(function(F){F.isLight&&F.layers.test(P.layers)&&(I.pushLight(F),F.castShadow&&I.pushShadow(F))}),I.setupLights();let O=new Set;return _.traverse(function(F){if(!(F.isMesh||F.isPoints||F.isLine||F.isSprite))return;let ce=F.material;if(ce)if(Array.isArray(ce))for(let he=0;he<ce.length;he++){let le=ce[he];Fg(le,z,F),O.add(le)}else Fg(ce,z,F),O.add(ce)}),I=A.pop(),O},this.compileAsync=function(_,P,z=null){let O=this.compile(_,P,z);return new Promise(F=>{function ce(){if(O.forEach(function(he){v.get(he).currentProgram.isReady()&&O.delete(he)}),O.size===0){F(_);return}setTimeout(ce,10)}lt.get("KHR_parallel_shader_compile")!==null?ce():setTimeout(ce,10)})};let hf=null;function DM(_){hf&&hf(_)}function Og(){Gr.stop()}function kg(){Gr.start()}let Gr=new EM;Gr.setAnimationLoop(DM),typeof self<"u"&&Gr.setContext(self),this.setAnimationLoop=function(_){hf=_,G.setAnimationLoop(_),_===null?Gr.stop():Gr.start()},G.addEventListener("sessionstart",Og),G.addEventListener("sessionend",kg),this.render=function(_,P){if(P!==void 0&&P.isCamera!==!0){Ne("WebGLRenderer.render: camera is not an instance of THREE.Camera.");return}if(q===!0)return;let z=G.enabled===!0&&G.isPresenting===!0,O=y!==null&&(V===null||z)&&y.begin(E,V);if(_.matrixWorldAutoUpdate===!0&&_.updateMatrixWorld(),P.parent===null&&P.matrixWorldAutoUpdate===!0&&P.updateMatrixWorld(),G.enabled===!0&&G.isPresenting===!0&&(y===null||y.isCompositing()===!1)&&(G.cameraAutoUpdate===!0&&G.updateCamera(P),P=G.getCamera()),_.isScene===!0&&_.onBeforeRender(E,_,P,V),I=J.get(_,A.length),I.init(P),A.push(I),Yt.multiplyMatrices(P.projectionMatrix,P.matrixWorldInverse),Ve.setFromProjectionMatrix(Yt,hi,P.reversedDepth),Fe=this.localClippingEnabled,Ie=ee.init(this.clippingPlanes,Fe),S=Re.get(_,D.length),S.init(),D.push(S),G.enabled===!0&&G.isPresenting===!0){let he=E.xr.getDepthSensingMesh();he!==null&&pf(he,P,-1/0,E.sortObjects)}pf(_,P,0,E.sortObjects),S.finish(),E.sortObjects===!0&&S.sort(Tt,St),We=G.enabled===!1||G.isPresenting===!1||G.hasDepthSensing()===!1,We&&xe.addToRenderList(S,_),this.info.render.frame++,Ie===!0&&ee.beginShadows();let F=I.state.shadowsArray;if(ye.render(F,_,P),Ie===!0&&ee.endShadows(),this.info.autoReset===!0&&this.info.reset(),(O&&y.hasRenderPass())===!1){let he=S.opaque,le=S.transmissive;if(I.setupLights(),P.isArrayCamera){let Me=P.cameras;if(le.length>0)for(let Se=0,Ue=Me.length;Se<Ue;Se++){let $e=Me[Se];Bg(he,le,_,$e)}We&&xe.render(_);for(let Se=0,Ue=Me.length;Se<Ue;Se++){let $e=Me[Se];Ug(S,_,$e,$e.viewport)}}else le.length>0&&Bg(he,le,_,P),We&&xe.render(_),Ug(S,_,P)}V!==null&&U===0&&(N.updateMultisampleRenderTarget(V),N.updateRenderTargetMipmap(V)),O&&y.end(E),_.isScene===!0&&_.onAfterRender(E,_,P),te.resetDefaultState(),W=-1,B=null,A.pop(),A.length>0?(I=A[A.length-1],Ie===!0&&ee.setGlobalState(E.clippingPlanes,I.state.camera)):I=null,D.pop(),D.length>0?S=D[D.length-1]:S=null};function pf(_,P,z,O){if(_.visible===!1)return;if(_.layers.test(P.layers)){if(_.isGroup)z=_.renderOrder;else if(_.isLOD)_.autoUpdate===!0&&_.update(P);else if(_.isLight)I.pushLight(_),_.castShadow&&I.pushShadow(_);else if(_.isSprite){if(!_.frustumCulled||Ve.intersectsSprite(_)){O&&ft.setFromMatrixPosition(_.matrixWorld).applyMatrix4(Yt);let he=ve.update(_),le=_.material;le.visible&&S.push(_,he,le,z,ft.z,null)}}else if((_.isMesh||_.isLine||_.isPoints)&&(!_.frustumCulled||Ve.intersectsObject(_))){let he=ve.update(_),le=_.material;if(O&&(_.boundingSphere!==void 0?(_.boundingSphere===null&&_.computeBoundingSphere(),ft.copy(_.boundingSphere.center)):(he.boundingSphere===null&&he.computeBoundingSphere(),ft.copy(he.boundingSphere.center)),ft.applyMatrix4(_.matrixWorld).applyMatrix4(Yt)),Array.isArray(le)){let Me=he.groups;for(let Se=0,Ue=Me.length;Se<Ue;Se++){let $e=Me[Se],Te=le[$e.materialIndex];Te&&Te.visible&&S.push(_,he,Te,z,ft.z,$e)}}else le.visible&&S.push(_,he,le,z,ft.z,null)}}let ce=_.children;for(let he=0,le=ce.length;he<le;he++)pf(ce[he],P,z,O)}function Ug(_,P,z,O){let{opaque:F,transmissive:ce,transparent:he}=_;I.setupLightsView(z),Ie===!0&&ee.setGlobalState(E.clippingPlanes,z),O&&be.viewport(H.copy(O)),F.length>0&&mc(F,P,z),ce.length>0&&mc(ce,P,z),he.length>0&&mc(he,P,z),be.buffers.depth.setTest(!0),be.buffers.depth.setMask(!0),be.buffers.color.setMask(!0),be.setPolygonOffset(!1)}function Bg(_,P,z,O){if((z.isScene===!0?z.overrideMaterial:null)!==null)return;if(I.state.transmissionRenderTarget[O.id]===void 0){let Te=lt.has("EXT_color_buffer_half_float")||lt.has("EXT_color_buffer_float");I.state.transmissionRenderTarget[O.id]=new Ln(1,1,{generateMipmaps:!0,type:Te?Ni:Un,minFilter:Vr,samples:Math.max(4,bt.samples),stencilBuffer:s,resolveDepthBuffer:!1,resolveStencilBuffer:!1,colorSpace:rt.workingColorSpace})}let ce=I.state.transmissionRenderTarget[O.id],he=O.viewport||H;ce.setSize(he.z*E.transmissionResolutionScale,he.w*E.transmissionResolutionScale);let le=E.getRenderTarget(),Me=E.getActiveCubeFace(),Se=E.getActiveMipmapLevel();E.setRenderTarget(ce),E.getClearColor(Z),ue=E.getClearAlpha(),ue<1&&E.setClearColor(16777215,.5),E.clear(),We&&xe.render(z);let Ue=E.toneMapping;E.toneMapping=pi;let $e=O.viewport;if(O.viewport!==void 0&&(O.viewport=void 0),I.setupLightsView(O),Ie===!0&&ee.setGlobalState(E.clippingPlanes,O),mc(_,z,O),N.updateMultisampleRenderTarget(ce),N.updateRenderTargetMipmap(ce),lt.has("WEBGL_multisampled_render_to_texture")===!1){let Te=!1;for(let mt=0,Ot=P.length;mt<Ot;mt++){let Dt=P[mt],{object:gt,geometry:on,material:Ee,group:Tn}=Dt;if(Ee.side===Ai&&gt.layers.test(O.layers)){let ot=Ee.side;Ee.side=xn,Ee.needsUpdate=!0,Vg(gt,z,O,on,Ee,Tn),Ee.side=ot,Ee.needsUpdate=!0,Te=!0}}Te===!0&&(N.updateMultisampleRenderTarget(ce),N.updateRenderTargetMipmap(ce))}E.setRenderTarget(le,Me,Se),E.setClearColor(Z,ue),$e!==void 0&&(O.viewport=$e),E.toneMapping=Ue}function mc(_,P,z){let O=P.isScene===!0?P.overrideMaterial:null;for(let F=0,ce=_.length;F<ce;F++){let he=_[F],{object:le,geometry:Me,group:Se}=he,Ue=he.material;Ue.allowOverride===!0&&O!==null&&(Ue=O),le.layers.test(z.layers)&&Vg(le,P,z,Me,Ue,Se)}}function Vg(_,P,z,O,F,ce){_.onBeforeRender(E,P,z,O,F,ce),_.modelViewMatrix.multiplyMatrices(z.matrixWorldInverse,_.matrixWorld),_.normalMatrix.getNormalMatrix(_.modelViewMatrix),F.onBeforeRender(E,P,z,O,_,ce),F.transparent===!0&&F.side===Ai&&F.forceSinglePass===!1?(F.side=xn,F.needsUpdate=!0,E.renderBufferDirect(z,P,O,F,_,ce),F.side=ir,F.needsUpdate=!0,E.renderBufferDirect(z,P,O,F,_,ce),F.side=Ai):E.renderBufferDirect(z,P,O,F,_,ce),_.onAfterRender(E,P,z,O,F,ce)}function gc(_,P,z){P.isScene!==!0&&(P=_t);let O=v.get(_),F=I.state.lights,ce=I.state.shadowsArray,he=F.state.version,le=ie.getParameters(_,F.state,ce,P,z),Me=ie.getProgramCacheKey(le),Se=O.programs;O.environment=_.isMeshStandardMaterial||_.isMeshLambertMaterial||_.isMeshPhongMaterial?P.environment:null,O.fog=P.fog;let Ue=_.isMeshStandardMaterial||_.isMeshLambertMaterial&&!_.envMap||_.isMeshPhongMaterial&&!_.envMap;O.envMap=$.get(_.envMap||O.environment,Ue),O.envMapRotation=O.environment!==null&&_.envMap===null?P.environmentRotation:_.envMapRotation,Se===void 0&&(_.addEventListener("dispose",ht),Se=new Map,O.programs=Se);let $e=Se.get(Me);if($e!==void 0){if(O.currentProgram===$e&&O.lightsStateVersion===he)return zg(_,le),$e}else le.uniforms=ie.getUniforms(_),_.onBeforeCompile(le,E),$e=ie.acquireProgram(le,Me),Se.set(Me,$e),O.uniforms=le.uniforms;let Te=O.uniforms;return(!_.isShaderMaterial&&!_.isRawShaderMaterial||_.clipping===!0)&&(Te.clippingPlanes=ee.uniform),zg(_,le),O.needsLights=RM(_),O.lightsStateVersion=he,O.needsLights&&(Te.ambientLightColor.value=F.state.ambient,Te.lightProbe.value=F.state.probe,Te.directionalLights.value=F.state.directional,Te.directionalLightShadows.value=F.state.directionalShadow,Te.spotLights.value=F.state.spot,Te.spotLightShadows.value=F.state.spotShadow,Te.rectAreaLights.value=F.state.rectArea,Te.ltc_1.value=F.state.rectAreaLTC1,Te.ltc_2.value=F.state.rectAreaLTC2,Te.pointLights.value=F.state.point,Te.pointLightShadows.value=F.state.pointShadow,Te.hemisphereLights.value=F.state.hemi,Te.directionalShadowMatrix.value=F.state.directionalShadowMatrix,Te.spotLightMatrix.value=F.state.spotLightMatrix,Te.spotLightMap.value=F.state.spotLightMap,Te.pointShadowMatrix.value=F.state.pointShadowMatrix),O.currentProgram=$e,O.uniformsList=null,$e}function Hg(_){if(_.uniformsList===null){let P=_.currentProgram.getUniforms();_.uniformsList=Fo.seqWithValue(P.seq,_.uniforms)}return _.uniformsList}function zg(_,P){let z=v.get(_);z.outputColorSpace=P.outputColorSpace,z.batching=P.batching,z.batchingColor=P.batchingColor,z.instancing=P.instancing,z.instancingColor=P.instancingColor,z.instancingMorph=P.instancingMorph,z.skinning=P.skinning,z.morphTargets=P.morphTargets,z.morphNormals=P.morphNormals,z.morphColors=P.morphColors,z.morphTargetsCount=P.morphTargetsCount,z.numClippingPlanes=P.numClippingPlanes,z.numIntersection=P.numClipIntersection,z.vertexAlphas=P.vertexAlphas,z.vertexTangents=P.vertexTangents,z.toneMapping=P.toneMapping}function IM(_,P,z,O,F){P.isScene!==!0&&(P=_t),N.resetTextureUnits();let ce=P.fog,he=O.isMeshStandardMaterial||O.isMeshLambertMaterial||O.isMeshPhongMaterial?P.environment:null,le=V===null?E.outputColorSpace:V.isXRRenderTarget===!0?V.texture.colorSpace:Es,Me=O.isMeshStandardMaterial||O.isMeshLambertMaterial&&!O.envMap||O.isMeshPhongMaterial&&!O.envMap,Se=$.get(O.envMap||he,Me),Ue=O.vertexColors===!0&&!!z.attributes.color&&z.attributes.color.itemSize===4,$e=!!z.attributes.tangent&&(!!O.normalMap||O.anisotropy>0),Te=!!z.morphAttributes.position,mt=!!z.morphAttributes.normal,Ot=!!z.morphAttributes.color,Dt=pi;O.toneMapped&&(V===null||V.isXRRenderTarget===!0)&&(Dt=E.toneMapping);let gt=z.morphAttributes.position||z.morphAttributes.normal||z.morphAttributes.color,on=gt!==void 0?gt.length:0,Ee=v.get(O),Tn=I.state.lights;if(Ie===!0&&(Fe===!0||_!==B)){let Zt=_===B&&O.id===W;ee.setState(O,_,Zt)}let ot=!1;O.version===Ee.__version?(Ee.needsLights&&Ee.lightsStateVersion!==Tn.state.version||Ee.outputColorSpace!==le||F.isBatchedMesh&&Ee.batching===!1||!F.isBatchedMesh&&Ee.batching===!0||F.isBatchedMesh&&Ee.batchingColor===!0&&F.colorTexture===null||F.isBatchedMesh&&Ee.batchingColor===!1&&F.colorTexture!==null||F.isInstancedMesh&&Ee.instancing===!1||!F.isInstancedMesh&&Ee.instancing===!0||F.isSkinnedMesh&&Ee.skinning===!1||!F.isSkinnedMesh&&Ee.skinning===!0||F.isInstancedMesh&&Ee.instancingColor===!0&&F.instanceColor===null||F.isInstancedMesh&&Ee.instancingColor===!1&&F.instanceColor!==null||F.isInstancedMesh&&Ee.instancingMorph===!0&&F.morphTexture===null||F.isInstancedMesh&&Ee.instancingMorph===!1&&F.morphTexture!==null||Ee.envMap!==Se||O.fog===!0&&Ee.fog!==ce||Ee.numClippingPlanes!==void 0&&(Ee.numClippingPlanes!==ee.numPlanes||Ee.numIntersection!==ee.numIntersection)||Ee.vertexAlphas!==Ue||Ee.vertexTangents!==$e||Ee.morphTargets!==Te||Ee.morphNormals!==mt||Ee.morphColors!==Ot||Ee.toneMapping!==Dt||Ee.morphTargetsCount!==on)&&(ot=!0):(ot=!0,Ee.__version=O.version);let Kn=Ee.currentProgram;ot===!0&&(Kn=gc(O,P,F));let vi=!1,Wr=!1,Ns=!1,xt=Kn.getUniforms(),tn=Ee.uniforms;if(be.useProgram(Kn.program)&&(vi=!0,Wr=!0,Ns=!0),O.id!==W&&(W=O.id,Wr=!0),vi||B!==_){be.buffers.depth.getReversed()&&_.reversedDepth!==!0&&(_._reversedDepth=!0,_.updateProjectionMatrix()),xt.setValue(w,"projectionMatrix",_.projectionMatrix),xt.setValue(w,"viewMatrix",_.matrixWorldInverse);let ur=xt.map.cameraPosition;ur!==void 0&&ur.setValue(w,st.setFromMatrixPosition(_.matrixWorld)),bt.logarithmicDepthBuffer&&xt.setValue(w,"logDepthBufFC",2/(Math.log(_.far+1)/Math.LN2)),(O.isMeshPhongMaterial||O.isMeshToonMaterial||O.isMeshLambertMaterial||O.isMeshBasicMaterial||O.isMeshStandardMaterial||O.isShaderMaterial)&&xt.setValue(w,"isOrthographic",_.isOrthographicCamera===!0),B!==_&&(B=_,Wr=!0,Ns=!0)}if(Ee.needsLights&&(Tn.state.directionalShadowMap.length>0&&xt.setValue(w,"directionalShadowMap",Tn.state.directionalShadowMap,N),Tn.state.spotShadowMap.length>0&&xt.setValue(w,"spotShadowMap",Tn.state.spotShadowMap,N),Tn.state.pointShadowMap.length>0&&xt.setValue(w,"pointShadowMap",Tn.state.pointShadowMap,N)),F.isSkinnedMesh){xt.setOptional(w,F,"bindMatrix"),xt.setOptional(w,F,"bindMatrixInverse");let Zt=F.skeleton;Zt&&(Zt.boneTexture===null&&Zt.computeBoneTexture(),xt.setValue(w,"boneTexture",Zt.boneTexture,N))}F.isBatchedMesh&&(xt.setOptional(w,F,"batchingTexture"),xt.setValue(w,"batchingTexture",F._matricesTexture,N),xt.setOptional(w,F,"batchingIdTexture"),xt.setValue(w,"batchingIdTexture",F._indirectTexture,N),xt.setOptional(w,F,"batchingColorTexture"),F._colorsTexture!==null&&xt.setValue(w,"batchingColorTexture",F._colorsTexture,N));let lr=z.morphAttributes;if((lr.position!==void 0||lr.normal!==void 0||lr.color!==void 0)&&de.update(F,z,Kn),(Wr||Ee.receiveShadow!==F.receiveShadow)&&(Ee.receiveShadow=F.receiveShadow,xt.setValue(w,"receiveShadow",F.receiveShadow)),(O.isMeshStandardMaterial||O.isMeshLambertMaterial||O.isMeshPhongMaterial)&&O.envMap===null&&P.environment!==null&&(tn.envMapIntensity.value=P.environmentIntensity),tn.dfgLUT!==void 0&&(tn.dfgLUT.value=jR()),Wr&&(xt.setValue(w,"toneMappingExposure",E.toneMappingExposure),Ee.needsLights&&AM(tn,Ns),ce&&O.fog===!0&&Ce.refreshFogUniforms(tn,ce),Ce.refreshMaterialUniforms(tn,O,ze,fe,I.state.transmissionRenderTarget[_.id]),Fo.upload(w,Hg(Ee),tn,N)),O.isShaderMaterial&&O.uniformsNeedUpdate===!0&&(Fo.upload(w,Hg(Ee),tn,N),O.uniformsNeedUpdate=!1),O.isSpriteMaterial&&xt.setValue(w,"center",F.center),xt.setValue(w,"modelViewMatrix",F.modelViewMatrix),xt.setValue(w,"normalMatrix",F.normalMatrix),xt.setValue(w,"modelMatrix",F.matrixWorld),O.isShaderMaterial||O.isRawShaderMaterial){let Zt=O.uniformsGroups;for(let ur=0,Ps=Zt.length;ur<Ps;ur++){let Gg=Zt[ur];me.update(Gg,Kn),me.bind(Gg,Kn)}}return Kn}function AM(_,P){_.ambientLightColor.needsUpdate=P,_.lightProbe.needsUpdate=P,_.directionalLights.needsUpdate=P,_.directionalLightShadows.needsUpdate=P,_.pointLights.needsUpdate=P,_.pointLightShadows.needsUpdate=P,_.spotLights.needsUpdate=P,_.spotLightShadows.needsUpdate=P,_.rectAreaLights.needsUpdate=P,_.hemisphereLights.needsUpdate=P}function RM(_){return _.isMeshLambertMaterial||_.isMeshToonMaterial||_.isMeshPhongMaterial||_.isMeshStandardMaterial||_.isShadowMaterial||_.isShaderMaterial&&_.lights===!0}this.getActiveCubeFace=function(){return C},this.getActiveMipmapLevel=function(){return U},this.getRenderTarget=function(){return V},this.setRenderTargetTextures=function(_,P,z){let O=v.get(_);O.__autoAllocateDepthBuffer=_.resolveDepthBuffer===!1,O.__autoAllocateDepthBuffer===!1&&(O.__useRenderToTexture=!1),v.get(_.texture).__webglTexture=P,v.get(_.depthTexture).__webglTexture=O.__autoAllocateDepthBuffer?void 0:z,O.__hasExternalTextures=!0},this.setRenderTargetFramebuffer=function(_,P){let z=v.get(_);z.__webglFramebuffer=P,z.__useDefaultFramebuffer=P===void 0};let NM=w.createFramebuffer();this.setRenderTarget=function(_,P=0,z=0){V=_,C=P,U=z;let O=null,F=!1,ce=!1;if(_){let le=v.get(_);if(le.__useDefaultFramebuffer!==void 0){be.bindFramebuffer(w.FRAMEBUFFER,le.__webglFramebuffer),H.copy(_.viewport),L.copy(_.scissor),Q=_.scissorTest,be.viewport(H),be.scissor(L),be.setScissorTest(Q),W=-1;return}else if(le.__webglFramebuffer===void 0)N.setupRenderTarget(_);else if(le.__hasExternalTextures)N.rebindTextures(_,v.get(_.texture).__webglTexture,v.get(_.depthTexture).__webglTexture);else if(_.depthBuffer){let Ue=_.depthTexture;if(le.__boundDepthTexture!==Ue){if(Ue!==null&&v.has(Ue)&&(_.width!==Ue.image.width||_.height!==Ue.image.height))throw new Error("WebGLRenderTarget: Attached DepthTexture is initialized to the incorrect size.");N.setupDepthRenderbuffer(_)}}let Me=_.texture;(Me.isData3DTexture||Me.isDataArrayTexture||Me.isCompressedArrayTexture)&&(ce=!0);let Se=v.get(_).__webglFramebuffer;_.isWebGLCubeRenderTarget?(Array.isArray(Se[P])?O=Se[P][z]:O=Se[P],F=!0):_.samples>0&&N.useMultisampledRTT(_)===!1?O=v.get(_).__webglMultisampledFramebuffer:Array.isArray(Se)?O=Se[z]:O=Se,H.copy(_.viewport),L.copy(_.scissor),Q=_.scissorTest}else H.copy(X).multiplyScalar(ze).floor(),L.copy(ne).multiplyScalar(ze).floor(),Q=se;if(z!==0&&(O=NM),be.bindFramebuffer(w.FRAMEBUFFER,O)&&be.drawBuffers(_,O),be.viewport(H),be.scissor(L),be.setScissorTest(Q),F){let le=v.get(_.texture);w.framebufferTexture2D(w.FRAMEBUFFER,w.COLOR_ATTACHMENT0,w.TEXTURE_CUBE_MAP_POSITIVE_X+P,le.__webglTexture,z)}else if(ce){let le=P;for(let Me=0;Me<_.textures.length;Me++){let Se=v.get(_.textures[Me]);w.framebufferTextureLayer(w.FRAMEBUFFER,w.COLOR_ATTACHMENT0+Me,Se.__webglTexture,z,le)}}else if(_!==null&&z!==0){let le=v.get(_.texture);w.framebufferTexture2D(w.FRAMEBUFFER,w.COLOR_ATTACHMENT0,w.TEXTURE_2D,le.__webglTexture,z)}W=-1},this.readRenderTargetPixels=function(_,P,z,O,F,ce,he,le=0){if(!(_&&_.isWebGLRenderTarget)){Ne("WebGLRenderer.readRenderTargetPixels: renderTarget is not THREE.WebGLRenderTarget.");return}let Me=v.get(_).__webglFramebuffer;if(_.isWebGLCubeRenderTarget&&he!==void 0&&(Me=Me[he]),Me){be.bindFramebuffer(w.FRAMEBUFFER,Me);try{let Se=_.textures[le],Ue=Se.format,$e=Se.type;if(_.textures.length>1&&w.readBuffer(w.COLOR_ATTACHMENT0+le),!bt.textureFormatReadable(Ue)){Ne("WebGLRenderer.readRenderTargetPixels: renderTarget is not in RGBA or implementation defined format.");return}if(!bt.textureTypeReadable($e)){Ne("WebGLRenderer.readRenderTargetPixels: renderTarget is not in UnsignedByteType or implementation defined type.");return}P>=0&&P<=_.width-O&&z>=0&&z<=_.height-F&&w.readPixels(P,z,O,F,re.convert(Ue),re.convert($e),ce)}finally{let Se=V!==null?v.get(V).__webglFramebuffer:null;be.bindFramebuffer(w.FRAMEBUFFER,Se)}}},this.readRenderTargetPixelsAsync=async function(_,P,z,O,F,ce,he,le=0){if(!(_&&_.isWebGLRenderTarget))throw new Error("THREE.WebGLRenderer.readRenderTargetPixels: renderTarget is not THREE.WebGLRenderTarget.");let Me=v.get(_).__webglFramebuffer;if(_.isWebGLCubeRenderTarget&&he!==void 0&&(Me=Me[he]),Me)if(P>=0&&P<=_.width-O&&z>=0&&z<=_.height-F){be.bindFramebuffer(w.FRAMEBUFFER,Me);let Se=_.textures[le],Ue=Se.format,$e=Se.type;if(_.textures.length>1&&w.readBuffer(w.COLOR_ATTACHMENT0+le),!bt.textureFormatReadable(Ue))throw new Error("THREE.WebGLRenderer.readRenderTargetPixelsAsync: renderTarget is not in RGBA or implementation defined format.");if(!bt.textureTypeReadable($e))throw new Error("THREE.WebGLRenderer.readRenderTargetPixelsAsync: renderTarget is not in UnsignedByteType or implementation defined type.");let Te=w.createBuffer();w.bindBuffer(w.PIXEL_PACK_BUFFER,Te),w.bufferData(w.PIXEL_PACK_BUFFER,ce.byteLength,w.STREAM_READ),w.readPixels(P,z,O,F,re.convert(Ue),re.convert($e),0);let mt=V!==null?v.get(V).__webglFramebuffer:null;be.bindFramebuffer(w.FRAMEBUFFER,mt);let Ot=w.fenceSync(w.SYNC_GPU_COMMANDS_COMPLETE,0);return w.flush(),await Jx(w,Ot,4),w.bindBuffer(w.PIXEL_PACK_BUFFER,Te),w.getBufferSubData(w.PIXEL_PACK_BUFFER,0,ce),w.deleteBuffer(Te),w.deleteSync(Ot),ce}else throw new Error("THREE.WebGLRenderer.readRenderTargetPixelsAsync: requested read bounds are out of range.")},this.copyFramebufferToTexture=function(_,P=null,z=0){let O=Math.pow(2,-z),F=Math.floor(_.image.width*O),ce=Math.floor(_.image.height*O),he=P!==null?P.x:0,le=P!==null?P.y:0;N.setTexture2D(_,0),w.copyTexSubImage2D(w.TEXTURE_2D,z,0,0,he,le,F,ce),be.unbindTexture()};let PM=w.createFramebuffer(),LM=w.createFramebuffer();this.copyTextureToTexture=function(_,P,z=null,O=null,F=0,ce=0){let he,le,Me,Se,Ue,$e,Te,mt,Ot,Dt=_.isCompressedTexture?_.mipmaps[ce]:_.image;if(z!==null)he=z.max.x-z.min.x,le=z.max.y-z.min.y,Me=z.isBox3?z.max.z-z.min.z:1,Se=z.min.x,Ue=z.min.y,$e=z.isBox3?z.min.z:0;else{let tn=Math.pow(2,-F);he=Math.floor(Dt.width*tn),le=Math.floor(Dt.height*tn),_.isDataArrayTexture?Me=Dt.depth:_.isData3DTexture?Me=Math.floor(Dt.depth*tn):Me=1,Se=0,Ue=0,$e=0}O!==null?(Te=O.x,mt=O.y,Ot=O.z):(Te=0,mt=0,Ot=0);let gt=re.convert(P.format),on=re.convert(P.type),Ee;P.isData3DTexture?(N.setTexture3D(P,0),Ee=w.TEXTURE_3D):P.isDataArrayTexture||P.isCompressedArrayTexture?(N.setTexture2DArray(P,0),Ee=w.TEXTURE_2D_ARRAY):(N.setTexture2D(P,0),Ee=w.TEXTURE_2D),w.pixelStorei(w.UNPACK_FLIP_Y_WEBGL,P.flipY),w.pixelStorei(w.UNPACK_PREMULTIPLY_ALPHA_WEBGL,P.premultiplyAlpha),w.pixelStorei(w.UNPACK_ALIGNMENT,P.unpackAlignment);let Tn=w.getParameter(w.UNPACK_ROW_LENGTH),ot=w.getParameter(w.UNPACK_IMAGE_HEIGHT),Kn=w.getParameter(w.UNPACK_SKIP_PIXELS),vi=w.getParameter(w.UNPACK_SKIP_ROWS),Wr=w.getParameter(w.UNPACK_SKIP_IMAGES);w.pixelStorei(w.UNPACK_ROW_LENGTH,Dt.width),w.pixelStorei(w.UNPACK_IMAGE_HEIGHT,Dt.height),w.pixelStorei(w.UNPACK_SKIP_PIXELS,Se),w.pixelStorei(w.UNPACK_SKIP_ROWS,Ue),w.pixelStorei(w.UNPACK_SKIP_IMAGES,$e);let Ns=_.isDataArrayTexture||_.isData3DTexture,xt=P.isDataArrayTexture||P.isData3DTexture;if(_.isDepthTexture){let tn=v.get(_),lr=v.get(P),Zt=v.get(tn.__renderTarget),ur=v.get(lr.__renderTarget);be.bindFramebuffer(w.READ_FRAMEBUFFER,Zt.__webglFramebuffer),be.bindFramebuffer(w.DRAW_FRAMEBUFFER,ur.__webglFramebuffer);for(let Ps=0;Ps<Me;Ps++)Ns&&(w.framebufferTextureLayer(w.READ_FRAMEBUFFER,w.COLOR_ATTACHMENT0,v.get(_).__webglTexture,F,$e+Ps),w.framebufferTextureLayer(w.DRAW_FRAMEBUFFER,w.COLOR_ATTACHMENT0,v.get(P).__webglTexture,ce,Ot+Ps)),w.blitFramebuffer(Se,Ue,he,le,Te,mt,he,le,w.DEPTH_BUFFER_BIT,w.NEAREST);be.bindFramebuffer(w.READ_FRAMEBUFFER,null),be.bindFramebuffer(w.DRAW_FRAMEBUFFER,null)}else if(F!==0||_.isRenderTargetTexture||v.has(_)){let tn=v.get(_),lr=v.get(P);be.bindFramebuffer(w.READ_FRAMEBUFFER,PM),be.bindFramebuffer(w.DRAW_FRAMEBUFFER,LM);for(let Zt=0;Zt<Me;Zt++)Ns?w.framebufferTextureLayer(w.READ_FRAMEBUFFER,w.COLOR_ATTACHMENT0,tn.__webglTexture,F,$e+Zt):w.framebufferTexture2D(w.READ_FRAMEBUFFER,w.COLOR_ATTACHMENT0,w.TEXTURE_2D,tn.__webglTexture,F),xt?w.framebufferTextureLayer(w.DRAW_FRAMEBUFFER,w.COLOR_ATTACHMENT0,lr.__webglTexture,ce,Ot+Zt):w.framebufferTexture2D(w.DRAW_FRAMEBUFFER,w.COLOR_ATTACHMENT0,w.TEXTURE_2D,lr.__webglTexture,ce),F!==0?w.blitFramebuffer(Se,Ue,he,le,Te,mt,he,le,w.COLOR_BUFFER_BIT,w.NEAREST):xt?w.copyTexSubImage3D(Ee,ce,Te,mt,Ot+Zt,Se,Ue,he,le):w.copyTexSubImage2D(Ee,ce,Te,mt,Se,Ue,he,le);be.bindFramebuffer(w.READ_FRAMEBUFFER,null),be.bindFramebuffer(w.DRAW_FRAMEBUFFER,null)}else xt?_.isDataTexture||_.isData3DTexture?w.texSubImage3D(Ee,ce,Te,mt,Ot,he,le,Me,gt,on,Dt.data):P.isCompressedArrayTexture?w.compressedTexSubImage3D(Ee,ce,Te,mt,Ot,he,le,Me,gt,Dt.data):w.texSubImage3D(Ee,ce,Te,mt,Ot,he,le,Me,gt,on,Dt):_.isDataTexture?w.texSubImage2D(w.TEXTURE_2D,ce,Te,mt,he,le,gt,on,Dt.data):_.isCompressedTexture?w.compressedTexSubImage2D(w.TEXTURE_2D,ce,Te,mt,Dt.width,Dt.height,gt,Dt.data):w.texSubImage2D(w.TEXTURE_2D,ce,Te,mt,he,le,gt,on,Dt);w.pixelStorei(w.UNPACK_ROW_LENGTH,Tn),w.pixelStorei(w.UNPACK_IMAGE_HEIGHT,ot),w.pixelStorei(w.UNPACK_SKIP_PIXELS,Kn),w.pixelStorei(w.UNPACK_SKIP_ROWS,vi),w.pixelStorei(w.UNPACK_SKIP_IMAGES,Wr),ce===0&&P.generateMipmaps&&w.generateMipmap(Ee),be.unbindTexture()},this.initRenderTarget=function(_){v.get(_).__webglFramebuffer===void 0&&N.setupRenderTarget(_)},this.initTexture=function(_){_.isCubeTexture?N.setTextureCube(_,0):_.isData3DTexture?N.setTexture3D(_,0):_.isDataArrayTexture||_.isCompressedArrayTexture?N.setTexture2DArray(_,0):N.setTexture2D(_,0),be.unbindTexture()},this.resetState=function(){C=0,U=0,V=null,be.reset(),te.reset()},typeof __THREE_DEVTOOLS__<"u"&&__THREE_DEVTOOLS__.dispatchEvent(new CustomEvent("observe",{detail:this}))}get coordinateSystem(){return hi}get outputColorSpace(){return this._outputColorSpace}set outputColorSpace(e){this._outputColorSpace=e;let t=this.getContext();t.drawingBufferColorSpace=rt._getDrawingBufferColorSpace(e),t.unpackColorSpace=rt._getUnpackColorSpace()}};var qR=["heroCanvas"];function XR(n,e){n&1&&zt(0,"canvas",3,0)}var tf=class n{heroCanvas;i18n=pe(Ct);content=this.i18n.content;cvPath=fo.cvPath;reducedMotion=ln(!1);supportsWebgl=ln(!0);showCanvas=Tr(()=>!this.reducedMotion()&&this.supportsWebgl());renderer;scene;camera;points;frameId;ngAfterViewInit(){let e=typeof window<"u"&&typeof window.matchMedia=="function";if(this.reducedMotion.set(e?window.matchMedia("(prefers-reduced-motion: reduce)").matches:!1),this.reducedMotion())return;let t=this.heroCanvas?.nativeElement;if(t)try{this.initScene(t),this.animate(),window.addEventListener("resize",this.onResize)}catch{this.supportsWebgl.set(!1),this.destroyScene()}}ngOnDestroy(){window.removeEventListener("resize",this.onResize),this.destroyScene()}initScene(e){let{width:t,height:i}=e.getBoundingClientRect();this.scene=new Wa,this.camera=new hn(50,t/Math.max(i,1),.1,100),this.camera.position.set(0,0,6),this.renderer=new Kd({canvas:e,antialias:!0,alpha:!0,powerPreference:"high-performance"}),this.renderer.setPixelRatio(Math.min(window.devicePixelRatio||1,1.8)),this.renderer.setSize(t,i,!1);let r=new Fn,s=t<768?420:720,o=new Float32Array(s*3),a=new Float32Array(s*3),c=[new Xe(5231045),new Xe(6333946),new Xe(16020150),new Xe(16096779),new Xe(3462041)];for(let f=0;f<s;f+=1){let d=f*3;o[d]=(Math.random()-.5)*10,o[d+1]=(Math.random()-.5)*6,o[d+2]=(Math.random()-.5)*4;let h=c[Math.floor(Math.random()*c.length)].clone();h.offsetHSL((Math.random()-.5)*.045,.06,.04),a[d]=h.r,a[d+1]=h.g,a[d+2]=h.b}r.setAttribute("position",new pn(o,3)),r.setAttribute("color",new pn(a,3));let l=new Io({vertexColors:!0,size:t<768?.05:.038,transparent:!0,opacity:.72,depthWrite:!1,blending:rr});this.points=new Za(r,l),this.scene.add(this.points);let u=new ic(16777215,.42);this.scene.add(u)}animate=()=>{!this.renderer||!this.scene||!this.camera||!this.points||(this.points.rotation.y+=45e-5,this.points.rotation.x+=15e-5,this.renderer.render(this.scene,this.camera),this.frameId=window.requestAnimationFrame(this.animate))};onResize=()=>{if(!this.renderer||!this.camera||!this.heroCanvas)return;let e=this.heroCanvas.nativeElement,{width:t,height:i}=e.getBoundingClientRect();this.renderer.setSize(t,i,!1),this.camera.aspect=t/Math.max(i,1),this.camera.updateProjectionMatrix()};destroyScene(){this.frameId&&window.cancelAnimationFrame(this.frameId),this.points?.geometry.dispose(),this.points?.material instanceof Ii&&this.points.material.dispose(),this.renderer?.dispose(),this.points=void 0,this.renderer=void 0,this.scene=void 0,this.camera=void 0,this.frameId=void 0}static \u0275fac=function(t){return new(t||n)};static \u0275cmp=Mt({type:n,selectors:[["app-hero"]],viewQuery:function(t,i){if(t&1&&jl(qR,5),t&2){let r;nm(r=im())&&(i.heroCanvas=r.first)}},decls:18,vars:20,consts:[["heroCanvas",""],["id","hero","aria-labelledby","hero-title",1,"hero"],["aria-hidden","true",1,"hero-bg"],[1,"hero-canvas"],[1,"hero-gradient"],[1,"container","hero-content"],[1,"hero-badge",3,"innerHTML"],["id","hero-title",3,"innerHTML"],[1,"hero-subtitle",3,"innerHTML"],[1,"hero-actions"],["href","#contact",1,"btn","btn-primary",3,"innerHTML"],["href","#projects",1,"btn","btn-ghost",3,"innerHTML"],["target","_blank","rel","noopener noreferrer",1,"btn","btn-ghost",3,"href","innerHTML"]],template:function(t,i){t&1&&(Ut(0,"section",1)(1,"div",2),ci(2,XR,2,0,"canvas",3),zt(3,"div",4),$t(),Ut(4,"div",5),zt(5,"p",6),nt(6,"richText"),zt(7,"h1",7),nt(8,"richText"),zt(9,"p",8),nt(10,"richText"),Ut(11,"div",9),zt(12,"a",10),nt(13,"richText"),zt(14,"a",11),nt(15,"richText"),zt(16,"a",12),nt(17,"richText"),$t()()()),t&2&&(oe(2),li(i.showCanvas()?2:-1),oe(3),rn("innerHTML",it(6,8,i.content().hero.badge),tt),oe(2),rn("innerHTML",it(8,10,i.content().hero.title),tt),oe(2),rn("innerHTML",it(10,12,i.content().hero.subtitle),tt),oe(3),rn("innerHTML",it(13,14,i.content().hero.ctaContact),tt),oe(2),rn("innerHTML",it(15,16,i.content().hero.ctaProjects),tt),oe(2),rn("href",i.cvPath,Xi)("innerHTML",it(17,18,i.content().hero.ctaCv),tt))},dependencies:[Nt],styles:[".hero[_ngcontent-%COMP%]{position:relative;overflow:clip;min-height:min(88vh,56rem);display:grid;align-items:center;padding-block:clamp(5rem,10vw,7.5rem) clamp(3.5rem,8vw,6rem)}.hero-bg[_ngcontent-%COMP%], .hero-canvas[_ngcontent-%COMP%], .hero-gradient[_ngcontent-%COMP%]{position:absolute;inset:0}.hero-canvas[_ngcontent-%COMP%]{width:100%;height:100%;opacity:1}.hero-gradient[_ngcontent-%COMP%]{background:radial-gradient(circle at 18% 18%,color-mix(in srgb,var(--accent) 12%,transparent),transparent 52%),linear-gradient(180deg,color-mix(in srgb,var(--surface-primary) 6%,transparent),var(--surface-primary))}.hero-content[_ngcontent-%COMP%]{position:relative;z-index:2;max-width:56rem}.hero-badge[_ngcontent-%COMP%]{display:inline-block;margin:0 0 1rem;padding:.35rem .72rem;border:1px solid var(--border-color);border-radius:999px;color:var(--text-secondary);font-size:1.5rem;letter-spacing:.08em;text-transform:uppercase;font-weight:700}h1[_ngcontent-%COMP%]{margin:0;font-family:var(--font-display);font-size:clamp(2rem,7vw,4rem);line-height:1.04;letter-spacing:-.03em;color:var(--text-primary)}.hero-subtitle[_ngcontent-%COMP%]{margin:1.2rem 0 0;max-width:52ch;color:var(--text-secondary);font-size:clamp(1rem,2.6vw,1.2rem)}.hero-actions[_ngcontent-%COMP%]{margin-top:2rem;display:flex;flex-wrap:wrap;gap:.7rem}"]})};var gn=class n{title=Ta.required();id=Ta();static \u0275fac=function(t){return new(t||n)};static \u0275cmp=Mt({type:n,selectors:[["app-section-title"]],inputs:{title:[1,"title"],id:[1,"id"]},decls:2,vars:4,consts:[[1,"section-title",3,"id","innerHTML"]],template:function(t,i){t&1&&(zt(0,"h2",0),nt(1,"richText")),t&2&&rn("id",i.id())("innerHTML",it(1,2,i.title()),tt)},dependencies:[Nt],styles:[".section-title[_ngcontent-%COMP%]{margin:0 0 1.25rem;font-family:var(--font-display);font-size:clamp(1.4rem,3vw,2rem);line-height:1.15;letter-spacing:-.02em;color:var(--text-primary)}"]})};var vn=class n{element=pe(br);renderer=pe(Hl);observer;ngOnInit(){if(typeof window<"u"&&typeof window.matchMedia=="function"&&window.matchMedia("(prefers-reduced-motion: reduce)").matches){this.renderer.addClass(this.element.nativeElement,"is-visible");return}if(typeof IntersectionObserver>"u"){this.renderer.addClass(this.element.nativeElement,"is-visible");return}this.observer=new IntersectionObserver(t=>{t.forEach(i=>{i.isIntersecting&&(this.renderer.addClass(i.target,"is-visible"),this.observer?.unobserve(i.target))})},{threshold:.16}),this.observer.observe(this.element.nativeElement)}ngOnDestroy(){this.observer?.disconnect()}static \u0275fac=function(t){return new(t||n)};static \u0275dir=xa({type:n,selectors:[["","appRevealOnScroll",""]]})};function YR(n,e){if(n&1&&(Je(0,"p",5),nt(1,"richText")),n&2){let t=e.$implicit;Ze("innerHTML",it(1,1,t),tt)}}var nf=class n{content=pe(Ct).content;static \u0275fac=function(t){return new(t||n)};static \u0275cmp=Mt({type:n,selectors:[["app-about"]],decls:8,vars:4,consts:[["id","about","aria-labelledby","about-title",1,"section"],["appRevealOnScroll","",1,"container"],["id","about-section-title",3,"title"],[1,"surface","about-surface"],["id","about-title",3,"innerHTML"],[3,"innerHTML"]],template:function(t,i){t&1&&(De(0,"section",0)(1,"div",1),Je(2,"app-section-title",2),De(3,"div",3),Je(4,"h3",4),nt(5,"richText"),Jt(6,YR,2,3,"p",5,Zi),Pe()()()),t&2&&(oe(2),Ze("title",i.content().sectionTitles.about),oe(2),Ze("innerHTML",it(5,2,i.content().about.title),tt),oe(2),Kt(i.content().about.paragraphs))},dependencies:[gn,vn,Nt],styles:[".about-surface[_ngcontent-%COMP%]   h3[_ngcontent-%COMP%]{margin:0 0 1rem;font-size:1.15rem;color:var(--text-primary)}.about-surface[_ngcontent-%COMP%]   p[_ngcontent-%COMP%]{margin:0;color:var(--text-secondary)}.about-surface[_ngcontent-%COMP%]   p[_ngcontent-%COMP%] + p[_ngcontent-%COMP%]{margin-top:.9rem}"]})};function ZR(n,e){if(n&1&&(zt(0,"li",1),nt(1,"richText")),n&2){let t=e.$implicit;rn("innerHTML",it(1,1,t),tt)}}var ko=class n{items=Ta.required();static \u0275fac=function(t){return new(t||n)};static \u0275cmp=Mt({type:n,selectors:[["app-chip-list"]],inputs:{items:[1,"items"]},decls:3,vars:0,consts:[[1,"chip-list"],[1,"chip",3,"innerHTML"]],template:function(t,i){t&1&&(Ut(0,"ul",0),Jt(1,ZR,2,3,"li",1,Zi),$t()),t&2&&(oe(),Kt(i.items()))},dependencies:[Nt],styles:[".chip-list[_ngcontent-%COMP%]{margin:0;padding:0;list-style:none;display:flex;flex-wrap:wrap;gap:.5rem}.chip[_ngcontent-%COMP%]{padding:.35rem .7rem;border:1px solid var(--border-color);border-radius:.5rem;background:color-mix(in srgb,var(--surface-secondary) 60%,transparent);color:var(--text-secondary);font-size:.82rem;letter-spacing:.02em}"]})};var JR=(n,e)=>e.title;function KR(n,e){if(n&1&&(De(0,"a",9),ut(1,"Demo"),Pe()),n&2){let t=En().$implicit;Ze("href",t.links.demo,Xi)}}function QR(n,e){if(n&1&&(De(0,"a",9),ut(1,"Github"),Pe()),n&2){let t=En().$implicit;Ze("href",t.links.repository,Xi)}}function eN(n,e){if(n&1&&(De(0,"article",5),Je(1,"h3",6),nt(2,"richText"),Je(3,"p",6),nt(4,"richText"),Je(5,"app-chip-list",7),De(6,"div",8),ci(7,KR,2,1,"a",9),ci(8,QR,2,1,"a",9),Pe()()),n&2){let t=e.$implicit;oe(),Ze("innerHTML",it(2,5,t.title),tt),oe(2),Ze("innerHTML",it(4,7,t.summary),tt),oe(2),Ze("items",t.stack),oe(2),li(t.links.demo?7:-1),oe(),li(t.links.repository?8:-1)}}var rf=class n{content=pe(Ct).content;static \u0275fac=function(t){return new(t||n)};static \u0275cmp=Mt({type:n,selectors:[["app-projects"]],decls:7,vars:1,consts:[["id","projects","aria-labelledby","projects-title",1,"section"],[1,"container"],["appRevealOnScroll",""],["id","projects-title",3,"title"],[1,"projects-grid"],["appRevealOnScroll","",1,"surface","project-card"],[3,"innerHTML"],[3,"items"],[1,"links"],["target","_blank","rel","noopener noreferrer",3,"href"]],template:function(t,i){t&1&&(De(0,"section",0)(1,"div",1)(2,"div",2),Je(3,"app-section-title",3),Pe(),De(4,"div",4),Jt(5,eN,9,9,"article",5,JR),Pe()()()),t&2&&(oe(3),Ze("title",i.content().sectionTitles.projects),oe(2),Kt(i.content().projects))},dependencies:[gn,ko,vn,Nt],styles:[".projects-grid[_ngcontent-%COMP%]{display:grid;grid-template-columns:repeat(1,minmax(0,1fr));gap:1rem}.project-card[_ngcontent-%COMP%]{display:grid;gap:.95rem;transition:transform .2s ease,border-color .2s ease}.project-card[_ngcontent-%COMP%]   h3[_ngcontent-%COMP%]{margin:0;font-size:1.1rem}.project-card[_ngcontent-%COMP%]   p[_ngcontent-%COMP%]{margin:0;color:var(--text-secondary)}.project-card[_ngcontent-%COMP%]:hover{border-color:color-mix(in srgb,var(--accent) 35%,var(--border-color));transform:translateY(-2px)}.links[_ngcontent-%COMP%]{display:flex;gap:.8rem}.links[_ngcontent-%COMP%]   a[_ngcontent-%COMP%]{color:var(--text-primary);font-size:.9rem;text-decoration:underline;text-underline-offset:.2em}@media(min-width:768px){.projects-grid[_ngcontent-%COMP%]{grid-template-columns:repeat(2,minmax(0,1fr))}}"]})};var tN=(n,e)=>e.category;function nN(n,e){if(n&1&&(De(0,"article",5)(1,"h3"),ut(2),Pe(),Je(3,"app-chip-list",6),Pe()),n&2){let t=e.$implicit;oe(2),un(t.category),oe(),Ze("items",t.items)}}var sf=class n{content=pe(Ct).content;static \u0275fac=function(t){return new(t||n)};static \u0275cmp=Mt({type:n,selectors:[["app-skills"]],decls:7,vars:1,consts:[["id","skills","aria-labelledby","skills-title",1,"section"],[1,"container"],["appRevealOnScroll",""],["id","skills-title",3,"title"],[1,"skills-grid"],["appRevealOnScroll","",1,"surface","skill-group"],[3,"items"]],template:function(t,i){t&1&&(De(0,"section",0)(1,"div",1)(2,"div",2),Je(3,"app-section-title",3),Pe(),De(4,"div",4),Jt(5,nN,4,2,"article",5,tN),Pe()()()),t&2&&(oe(3),Ze("title",i.content().sectionTitles.skills),oe(2),Kt(i.content().skills))},dependencies:[gn,ko,vn],styles:[".skills-grid[_ngcontent-%COMP%]{display:grid;grid-template-columns:repeat(1,minmax(0,1fr));gap:1rem}.skill-group[_ngcontent-%COMP%]{display:grid;gap:.95rem}.skill-group[_ngcontent-%COMP%]   h3[_ngcontent-%COMP%]{margin:0;font-size:1rem;color:var(--text-primary);letter-spacing:.02em}@media(min-width:768px){.skills-grid[_ngcontent-%COMP%]{grid-template-columns:repeat(3,minmax(0,1fr))}}"]})};var iN=(n,e)=>e.role+e.period;function rN(n,e){if(n&1&&(Je(0,"li",6),nt(1,"richText")),n&2){let t=e.$implicit;Ze("innerHTML",it(1,1,t),tt)}}function sN(n,e){if(n&1&&(De(0,"article",5)(1,"header"),Je(2,"h3",6),nt(3,"richText"),Je(4,"p",6),nt(5,"richText"),Je(6,"span",6),nt(7,"richText"),Pe(),De(8,"ul"),Jt(9,rN,2,3,"li",6,Zi),Pe()()),n&2){let t=e.$implicit;oe(2),Ze("innerHTML",it(3,3,t.role),tt),oe(2),Ze("innerHTML",it(5,5,t.company),tt),oe(2),Ze("innerHTML",it(7,7,t.period),tt),oe(3),Kt(t.details)}}var of=class n{content=pe(Ct).content;static \u0275fac=function(t){return new(t||n)};static \u0275cmp=Mt({type:n,selectors:[["app-experience"]],decls:7,vars:1,consts:[["id","experience","aria-labelledby","experience-title",1,"section"],[1,"container"],["appRevealOnScroll",""],["id","experience-title",3,"title"],[1,"timeline"],["appRevealOnScroll","",1,"surface","timeline-item"],[3,"innerHTML"]],template:function(t,i){t&1&&(De(0,"section",0)(1,"div",1)(2,"div",2),Je(3,"app-section-title",3),Pe(),De(4,"div",4),Jt(5,sN,11,9,"article",5,iN),Pe()()()),t&2&&(oe(3),Ze("title",i.content().sectionTitles.experience),oe(2),Kt(i.content().experiences))},dependencies:[gn,vn,Nt],styles:[".timeline[_ngcontent-%COMP%]{display:grid;gap:1rem}.timeline-item[_ngcontent-%COMP%]{display:grid;gap:.8rem}.timeline-item[_ngcontent-%COMP%]   header[_ngcontent-%COMP%]{display:grid;gap:.25rem}.timeline-item[_ngcontent-%COMP%]   h3[_ngcontent-%COMP%]{margin:0;font-size:1.05rem}.timeline-item[_ngcontent-%COMP%]   p[_ngcontent-%COMP%]{margin:0;color:var(--text-secondary)}.timeline-item[_ngcontent-%COMP%]   span[_ngcontent-%COMP%]{font-size:.84rem;color:var(--text-tertiary)}.timeline-item[_ngcontent-%COMP%]   ul[_ngcontent-%COMP%]{margin:0;padding-left:1rem;color:var(--text-secondary);display:grid;gap:.35rem}"]})};function oN(n,e){if(n&1){let t=lo();De(0,"button",10),uo("click",function(){yr(t);let r=En();return _r(r.requestEmailAccess())}),ut(1," Afficher l'email "),Pe()}if(n&2){let t=En();Yi("aria-label",t.content().contact.emailLabel)}}function aN(n,e){n&1&&(De(0,"p",15),ut(1,"Reponse incorrecte, reessaye."),Pe())}function cN(n,e){if(n&1){let t=lo();De(0,"div",7)(1,"label",11),ut(2),Pe(),De(3,"div",12)(4,"input",13),uo("input",function(r){yr(t);let s=En();return _r(s.updateCaptchaAnswer(r.target.value))}),Pe(),De(5,"button",14),uo("click",function(){yr(t);let r=En();return _r(r.validateCaptcha())}),ut(6,"Valider"),Pe()(),ci(7,aN,2,0,"p",15),Pe()}if(n&2){let t=En();oe(2),$l("",t.captchaA()," + ",t.captchaB()," = ?"),oe(2),Ze("value",t.captchaAnswer()),oe(3),li(t.captchaError()?7:-1)}}function lN(n,e){n&1&&(De(0,"a",8),ut(1," philyscor@gmail.com "),Pe())}var af=class n{content=pe(Ct).content;captchaVisible=ln(!1);emailUnlocked=ln(!1);captchaA=ln(0);captchaB=ln(0);captchaAnswer=ln("");captchaError=ln(!1);requestEmailAccess(){this.captchaVisible.set(!0),this.emailUnlocked.set(!1),this.captchaAnswer.set(""),this.captchaError.set(!1),this.generateCaptcha()}updateCaptchaAnswer(e){this.captchaAnswer.set(e),this.captchaError.set(!1)}validateCaptcha(){let e=this.captchaA()+this.captchaB();if(Number(this.captchaAnswer())===e){this.emailUnlocked.set(!0),this.captchaVisible.set(!1),this.captchaError.set(!1);return}this.captchaError.set(!0),this.generateCaptcha()}generateCaptcha(){this.captchaA.set(Math.floor(Math.random()*8)+2),this.captchaB.set(Math.floor(Math.random()*8)+2)}static \u0275fac=function(t){return new(t||n)};static \u0275cmp=Mt({type:n,selectors:[["app-contact"]],decls:30,vars:19,consts:[["id","contact","aria-labelledby","contact-title",1,"section"],["appRevealOnScroll","",1,"container"],["id","contact-title",3,"title"],[1,"surface","contact-surface"],[3,"innerHTML"],[1,"contact-list"],["type","button",1,"mail-link"],["role","group","aria-label","Captcha email",1,"captcha-box"],["href","mailto:philyscor@gmail.com","aria-label","Email",1,"mail-link"],["target","_blank","rel","noopener noreferrer",3,"href"],["type","button",1,"mail-link",3,"click"],["for","captcha-answer"],[1,"captcha-actions"],["id","captcha-answer","type","number","inputmode","numeric",3,"input","value"],["type","button",1,"captcha-validate",3,"click"],[1,"captcha-error"]],template:function(t,i){t&1&&(De(0,"section",0)(1,"div",1),Je(2,"app-section-title",2),De(3,"div",3),Je(4,"h3",4),nt(5,"richText"),Je(6,"p",4),nt(7,"richText"),De(8,"ul",5)(9,"li")(10,"span"),ut(11),Pe(),ci(12,oN,2,1,"button",6),ci(13,cN,8,4,"div",7),ci(14,lN,2,0,"a",8),Pe(),De(15,"li")(16,"span"),ut(17),Pe(),De(18,"a",9),ut(19),Pe()(),De(20,"li")(21,"span"),ut(22),Pe(),De(23,"a",9),ut(24),Pe()(),De(25,"li")(26,"span"),ut(27),Pe(),De(28,"strong"),ut(29),Pe()()()()()()),t&2&&(oe(2),Ze("title",i.content().sectionTitles.contact),oe(2),Ze("innerHTML",it(5,15,i.content().contact.title),tt),oe(2),Ze("innerHTML",it(7,17,i.content().contact.intro),tt),oe(5),un(i.content().contact.emailLabel),oe(),li(i.emailUnlocked()?-1:12),oe(),li(i.captchaVisible()?13:-1),oe(),li(i.emailUnlocked()?14:-1),oe(3),un(i.content().contact.linkedinLabel),oe(),Ze("href",i.content().contact.linkedin,Xi),oe(),un(i.content().contact.linkedin),oe(3),un(i.content().contact.githubLabel),oe(),Ze("href",i.content().contact.github,Xi),oe(),un(i.content().contact.github),oe(3),un(i.content().contact.locationLabel),oe(2),un(i.content().contact.location))},dependencies:[gn,vn,Nt],styles:[".contact-surface[_ngcontent-%COMP%]   h3[_ngcontent-%COMP%]{margin:0;font-size:1.15rem}.contact-surface[_ngcontent-%COMP%]   p[_ngcontent-%COMP%]{margin:.8rem 0 1.2rem;color:var(--text-secondary)}.contact-list[_ngcontent-%COMP%]{margin:0;padding:0;list-style:none;display:grid;gap:.65rem}.contact-list[_ngcontent-%COMP%]   li[_ngcontent-%COMP%]{display:grid;gap:.15rem}.contact-list[_ngcontent-%COMP%]   span[_ngcontent-%COMP%]{font-size:.82rem;letter-spacing:.04em;text-transform:uppercase;color:var(--text-tertiary)}.contact-list[_ngcontent-%COMP%]   a[_ngcontent-%COMP%], .contact-list[_ngcontent-%COMP%]   strong[_ngcontent-%COMP%]{color:var(--text-primary);font-weight:500;width:fit-content}.contact-list[_ngcontent-%COMP%]   .mail-link[_ngcontent-%COMP%]{border:none;padding:0;background:transparent;color:var(--text-primary);font:inherit;font-weight:500;cursor:pointer;width:fit-content;text-align:left;text-decoration:underline;text-underline-offset:.18em}.contact-list[_ngcontent-%COMP%]   .captcha-box[_ngcontent-%COMP%]{display:grid;gap:.45rem;margin-top:.35rem}.contact-list[_ngcontent-%COMP%]   .captcha-box[_ngcontent-%COMP%]   label[_ngcontent-%COMP%]{color:var(--text-secondary);font-size:.9rem}.contact-list[_ngcontent-%COMP%]   .captcha-actions[_ngcontent-%COMP%]{display:flex;gap:.5rem;align-items:center}.contact-list[_ngcontent-%COMP%]   .captcha-actions[_ngcontent-%COMP%]   input[_ngcontent-%COMP%]{width:5.2rem;padding:.35rem .45rem;border:1px solid var(--border-color);border-radius:.45rem;background:var(--surface-primary);color:var(--text-primary)}.contact-list[_ngcontent-%COMP%]   .captcha-validate[_ngcontent-%COMP%]{border:1px solid var(--border-color);background:var(--surface-secondary);color:var(--text-primary);border-radius:.45rem;padding:.35rem .55rem;cursor:pointer}.contact-list[_ngcontent-%COMP%]   .captcha-error[_ngcontent-%COMP%]{margin:0;color:#c2410c;font-size:.85rem}"]})};var cf=class n{content=pe(Ct).content;year=new Date().getFullYear();static \u0275fac=function(t){return new(t||n)};static \u0275cmp=Mt({type:n,selectors:[["app-footer"]],decls:6,vars:4,consts:[[1,"footer"],[1,"container"],[3,"innerHTML"]],template:function(t,i){t&1&&(Ut(0,"footer",0)(1,"div",1)(2,"p"),ut(3),zt(4,"span",2),nt(5,"richText"),$t()()()),t&2&&(oe(3),Ea("",i.year," Portfolio. "),oe(),rn("innerHTML",it(5,2,i.content().footer.rights),tt))},dependencies:[Nt],styles:[".footer[_ngcontent-%COMP%]{padding:2.2rem 0 3rem;border-top:1px solid var(--border-color)}.footer[_ngcontent-%COMP%]   p[_ngcontent-%COMP%]{margin:0;color:var(--text-tertiary);font-size:.86rem}"]})};var lf=class n{title=pe(j0);meta=pe(W0);i18n=pe(Ct);constructor(){Uh(()=>{let e=this.i18n.language(),t=fo.seo[e];this.title.setTitle(t.title),this.meta.updateTag({name:"description",content:t.description}),this.meta.updateTag({property:"og:title",content:t.title}),this.meta.updateTag({property:"og:description",content:t.description})})}static \u0275fac=function(t){return new(t||n)};static \u0275prov=at({token:n,factory:n.\u0275fac,providedIn:"root"})};var uN=(n,e)=>e.diplome+e.period;function dN(n,e){if(n&1&&(De(0,"span",9),ut(1),Pe()),n&2){let t=En(2);Ze("title",t.content().labels.inProgress),Yi("aria-label",t.content().labels.inProgress),oe(),un(t.content().labels.inProgress)}}function fN(n,e){if(n&1&&(De(0,"article",6)(1,"header"),Je(2,"h3",7),nt(3,"richText"),Gl(4,dN,2,3,"span",8),Je(5,"p",7),nt(6,"richText"),Je(7,"span",7),nt(8,"richText"),Pe()()),n&2){let t=e.$implicit;Sr("in-progress",t.actual),oe(2),Ze("innerHTML",it(3,6,t.diplome),tt),oe(2),Ze("ngIf",t.actual),oe(),Ze("innerHTML",it(6,8,t.school),tt),oe(2),Ze("innerHTML",it(8,10,t.period),tt)}}var uf=class n{content=pe(Ct).content;static \u0275fac=function(t){return new(t||n)};static \u0275cmp=Mt({type:n,selectors:[["app-school"]],decls:7,vars:1,consts:[["id","school","aria-labelledby","school-title",1,"section"],[1,"container"],["appRevealOnScroll",""],["id","school-title",3,"title"],[1,"timeline"],["appRevealOnScroll","",1,"surface","timeline-item",3,"in-progress"],["appRevealOnScroll","",1,"surface","timeline-item"],[3,"innerHTML"],["class","status-badge",3,"title",4,"ngIf"],[1,"status-badge",3,"title"]],template:function(t,i){t&1&&(De(0,"section",0)(1,"div",1)(2,"div",2),Je(3,"app-section-title",3),Pe(),De(4,"div",4),Jt(5,fN,9,12,"article",5,uN),Pe()()()),t&2&&(oe(3),Ze("title",i.content().sectionTitles.school),oe(2),Kt(i.content().school))},dependencies:[Zl,om,gn,vn,Nt],styles:[".timeline[_ngcontent-%COMP%]{display:grid;gap:1rem}.timeline-item[_ngcontent-%COMP%]{display:grid;gap:.8rem;position:relative}.timeline-item[_ngcontent-%COMP%]   header[_ngcontent-%COMP%]{display:grid;gap:.25rem;padding-right:3.2rem}.timeline-item[_ngcontent-%COMP%]   h3[_ngcontent-%COMP%]{margin:0;font-size:1.05rem}.timeline-item[_ngcontent-%COMP%]   p[_ngcontent-%COMP%]{margin:0;color:var(--text-secondary)}.timeline-item[_ngcontent-%COMP%]   span[_ngcontent-%COMP%]{font-size:.84rem;color:var(--text-tertiary)}.timeline-item[_ngcontent-%COMP%]   ul[_ngcontent-%COMP%]{margin:0;padding-left:1rem;color:var(--text-secondary);display:grid;gap:.35rem}.timeline-item.in-progress[_ngcontent-%COMP%]{border-left:4px solid var(--accent);padding-left:.8rem;background:color-mix(in srgb,var(--accent) 8%,transparent)}.timeline-item[_ngcontent-%COMP%]   .status-badge[_ngcontent-%COMP%]{display:inline-block;position:absolute;top:.6rem;right:.6rem;padding:.18rem .6rem;font-size:.72rem;font-weight:600;border-radius:999px;z-index:2}[data-theme=light][_ngcontent-%COMP%]   .timeline-item[_ngcontent-%COMP%]   .status-badge[_ngcontent-%COMP%]{background:color-mix(in srgb,var(--accent) 18%,#ffffff);color:var(--text-primary);border:1px solid color-mix(in srgb,var(--accent) 40%,#cccccc);box-shadow:0 1px 2px #0a141e0f}[data-theme=dark][_ngcontent-%COMP%]   .timeline-item[_ngcontent-%COMP%]   .status-badge[_ngcontent-%COMP%]{background:var(--accent);color:#f5f8fc;border:none;box-shadow:0 1px 2px #0000004d}@media(max-width:600px){.timeline-item[_ngcontent-%COMP%]{padding-left:.6rem}.timeline-item[_ngcontent-%COMP%]   header[_ngcontent-%COMP%]{padding-right:0;display:grid;grid-template-columns:1fr;gap:.25rem .25rem;align-items:start}.timeline-item[_ngcontent-%COMP%]   .status-badge[_ngcontent-%COMP%]{position:static;grid-column:1;grid-row:2;justify-self:start;padding:.12rem .5rem;font-size:.78rem}.timeline-item[_ngcontent-%COMP%]   header[_ngcontent-%COMP%]   h3[_ngcontent-%COMP%], .timeline-item[_ngcontent-%COMP%]   header[_ngcontent-%COMP%]   p[_ngcontent-%COMP%], .timeline-item[_ngcontent-%COMP%]   header[_ngcontent-%COMP%]   span[_ngcontent-%COMP%]{grid-column:1}}"]})};function hN(n,e){if(n&1&&(De(0,"span",5),ut(1),Pe()),n&2){let t=e.$implicit;oe(),un(t)}}var df=class n{content=pe(Ct).content;static \u0275fac=function(t){return new(t||n)};static \u0275cmp=Mt({type:n,selectors:[["app-interests"]],decls:7,vars:1,consts:[["id","interests","aria-labelledby","interests-title",1,"section"],["appRevealOnScroll","",1,"container"],["id","interests-title",3,"title"],[1,"surface","interests-surface"],[1,"interests-list"],[1,"interest-chip"]],template:function(t,i){t&1&&(De(0,"section",0)(1,"div",1),Je(2,"app-section-title",2),De(3,"div",3)(4,"div",4),Jt(5,hN,2,1,"span",5,Zi),Pe()()()()),t&2&&(oe(2),Ze("title",i.content().sectionTitles.interests),oe(3),Kt(i.content().interests))},dependencies:[gn,vn],styles:[".interests-list[_ngcontent-%COMP%]{display:flex;flex-wrap:wrap;gap:.5rem;align-items:center}.interest-chip[_ngcontent-%COMP%]{display:inline-block;padding:.4rem .7rem;border-radius:999px;background:var(--chip-bg, #222);color:var(--chip-color, #fff);font-size:.9rem;box-shadow:0 1px #0000000a}[_ngcontent-%COMP%]:root[data-theme=light]   .interest-chip[_ngcontent-%COMP%]{--chip-bg: #e6f0ff;--chip-color: #0b2545}[_ngcontent-%COMP%]:root[data-theme=dark]   .interest-chip[_ngcontent-%COMP%]{--chip-bg: #2b394b;--chip-color: #eaf2ff}@media(max-width:480px){.interests-list[_ngcontent-%COMP%]{gap:.4rem}.interest-chip[_ngcontent-%COMP%]{font-size:.85rem;padding:.35rem .5rem}}"]})};var ff=class n{seo=pe(lf);constructor(){this.seo}static \u0275fac=function(t){return new(t||n)};static \u0275cmp=Mt({type:n,selectors:[["app-root"]],decls:11,vars:0,template:function(t,i){t&1&&(Je(0,"app-header"),De(1,"main"),Je(2,"app-hero")(3,"app-about")(4,"app-interests")(5,"app-projects")(6,"app-skills")(7,"app-experience")(8,"app-school")(9,"app-contact"),Pe(),Je(10,"app-footer"))},dependencies:[iu,tf,nf,rf,uf,sf,of,af,cf,df],styles:["[_nghost-%COMP%]{display:block}main[_ngcontent-%COMP%]{display:block}"]})};mm(ff,$0).catch(n=>console.error(n));
