export default {
  async fetch(request, env) {
    return new Response(
      JSON.stringify({
        status: "FTT Backend Running",
        time: new Date().toISOString()
      }),
      { headers: { "Content-Type": "application/json" } }
    );
  }
};
