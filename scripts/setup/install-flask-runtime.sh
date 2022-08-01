#!/bin/zsh

cd "$(dirname "$0")"

set -e
# set -x

source $HOME/anaconda3/bin/activate

# Source: https://www.linode.com/docs/guides/flask-and-gunicorn-on-ubuntu/

install_nginx()
{
    sudo apt-get install -y nginx
}

setup_nginx_site()
{
    nginx_site_name="electricity_data_rest_api"
    nginx_sites_dir=/etc/nginx/sites-enabled/

    echo >&2 "Setting up site $nginx_site_name ..."
    sudo cp conf/nginx/$nginx_site_name $nginx_sites_dir
    sudo sed -i "s/<hostname>/$(hostname)/" $nginx_sites_dir/$nginx_site_name
    echo >&2 "Disabling default nginx site ..."
    { set +e; sudo unlink /etc/nginx/sites-enabled/default }
    echo >&2 "Reloading nginx ..."
    sudo nginx -s reload
    echo >&2 "Done"
}

install_flask()
{
    # Or use pip
    conda create -n flask python
    conda activate flask
    conda install -c conda-forge flask-restful webargs
}

install_gunicorn()
{
    conda install -c anaconda gunicorn
}

install_supervisor()
{
    sudo apt-get install -y supervisor
}

setup_supervisor_app()
{
    sudo rsync -au conf/supervisor/electricity_data_rest_api.conf /etc/supervisor/conf.d/
}

main()
{
    install_nginx
    setup_nginx_site
    install_flask
    install_gunicorn
    install_supervisor
    setup_supervisor_app
}

main
