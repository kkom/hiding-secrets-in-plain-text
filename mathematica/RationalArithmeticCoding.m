(* ::Package:: *)

(************************************************************************)
(* This file was generated automatically by the Mathematica front end.  *)
(* It contains Initialization cells from a Notebook file, which         *)
(* typically will have the same name as this file except ending in      *)
(* ".nb" instead of ".m".                                               *)
(*                                                                      *)
(* This file is intended to be loaded into the Mathematica kernel using *)
(* the package loading commands Get or Needs.  Doing so is equivalent   *)
(* to using the Evaluate Initialization Cells menu command in the front *)
(* end.                                                                 *)
(*                                                                      *)
(* DO NOT EDIT THIS FILE.  This entire file is regenerated              *)
(* automatically each time the parent Notebook file is saved in the     *)
(* Mathematica front end.  Any changes you make to this file will be    *)
(* overwritten.                                                         *)
(************************************************************************)



BeginPackage["RationalArithmeticCoding`"]

RationalACEncoder::usage="RationalACEncoder[p,c1] generates a Rational Arithmetic Coding sequence encoder. Functions p[s,S] and c1[s,S] are conditional probability mass functions of the symbol s given preceding sequence S."

RationalACDecoder::usage=
"RationalACEncoder[matchingInterval,stop,p,c1] generates a Rational Arithmetic Coding interval decoder. Function \[Psi][v,S] gives the next symbol given current rescaled interval and preceding symbols sequence S. Function stop[v,S] decides whether a sequence S with rescaled interval v should be terminated. Functions p[s,S] and c1[s,S] are conditional probability mass functions of the symbol s given preceding sequence S."

Begin["`Private`"]

encoder[sequence_,p_,c1_]:=Module[
{\[CapitalPhi]},
\[CapitalPhi][{}]={0,1};
\[CapitalPhi][S_]:=\[CapitalPhi][S]=\[CapitalPhi][Most[S]].({
 {1, 0},
 {c1[Last[S],Most[S]], p[Last[S],Most[S]]}
});
\[CapitalPhi][sequence]
]

decoder[interval_,\[Psi]_,stop_,p_,c1_]:=Module[
{\[CapitalPsi],rescale},
rescale[v_,s_,S_]:={(First[v]-c1[s,S])/p[s,S],Last[v]/p[s,S]};
\[CapitalPsi][v_]:=\[CapitalPsi][
rescale[v,\[Psi][v,{}],{}],
{\[Psi][v,{}]}
];
\[CapitalPsi][v_,S_]:=\[CapitalPsi][v,S]=
If[
stop[v,S],
S,
\[CapitalPsi][rescale[v,\[Psi][v,S],S],Append[S,\[Psi][v,S]]]
];
\[CapitalPsi][interval]
]

RationalACEncoder[p_,c1_]:=Function[{S},encoder[S,p,c1]]
RationalACDecoder[matchingInterval_,stop_,p_,c1_]:=Function[{v},decoder[v,matchingInterval,stop,p,c1]]

End[]

EndPackage[]
